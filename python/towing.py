"""
Preprocesses data from the IVIC towing database, mainly by calculating the number of vehicles on lot, and their average
age per date. This generates a CSV file that is then used by the Shiny towing dashboard.
"""

import argparse
import csv
import datetime
import logging
import re

from collections import Counter, defaultdict
from filelock import FileLock, Timeout
import pyodbc

from namedlist import namedlist

logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

TOW_CATEGORIES = {
    0: 'total',
    111: 'police_action',
    112: 'accident',
    113: 'abandoned',
    125: 'scofflaw',
    140: 'impound',
    200: 'stolen_recovered',
    300: 'commercial_vehicle_restriction',
    1000: 'nocode'
}


class TowingData:
    """
    Manages towing database, data processing, and writing files
    """
    def __init__(self):
        conn = pyodbc.connect(r"Driver={SQL Server};"  # pylint:disable=c-extension-no-member
                              r"Server=DOT-FS04-SRV\DOT_FS04;"
                              r"Database=IVIC;"
                              r"Trusted_Connection=yes;")

        self.cursor = conn.cursor()

        data_categories = [f(x) for x in TOW_CATEGORIES.values() for f in (self._app_num, self._app_age)]

        DataAccumulator = namedlist('DataAccumulator', data_categories, default=0)

        # Uses the form of datetime: DataAccumulator
        self.date_hash = defaultdict(lambda: DataAccumulator())  # pylint:disable=unnecessary-lambda

    def get_all_vehicles(self):
        """
        Get all-time vechiles from the database

        :return: All rows from database with vehicle information
        """
        logging.info("Get_all_vehicles")
        self.cursor.execute('SELECT Vehicle_Release.Property_Number, Receiving_Date_Time, Release_Date_Time, '
                            'Pickup_Code, Pickup_Code_Change_Date, Original_Pickup_Code '
                            'FROM [Vehicle_Release] '
                            'JOIN Vehicle_Receiving '
                            'ON [Vehicle_Receiving].Property_Number = Vehicle_Release.Property_Number')
        return self.cursor.fetchall()

    def get_receive_date(self, property_number):
        """
         Get the receiving date for a specific property number

        :param property_number: property_number of the vehicle to look up
        :return: Datetime.date of the receiving_date
        """
        logging.info("get_receive_date")
        self.cursor.execute("SELECT Receiving_Date_Time FROM Vehicle_Receiving WHERE Property_Number=?",
                            property_number)
        res = self.cursor.fetchall()
        assert len(res) <= 1
        return res[0][0].date()

    def get_release_date(self, property_number):
        """
        Get the release date for a specific property number

        :param property_number: property_number of the vehicle to look up
        :return: Datetime.date of the release_date
        """
        self.cursor.execute("SELECT Release_Date_Time FROM Vehicle_Release WHERE Property_Number=?", property_number)
        res = self.cursor.fetchall()
        assert len(res) <= 1
        return res[0][0].date()

    def get_pickup_types(self, on_lot=True):
        """
        Counts the number of vehicles on lot based on their receiving code

        :param on_lot: true if we should only look at current vehicles; false if we should look at all time vehicles
        :return: dict of {pickupcode: quantity}
        """
        logging.info("get_pickup_types")
        if on_lot:
            self.cursor.execute("SELECT Vehicle_Receiving.Pickup_Code, Release_Date_Time "
                                "FROM Vehicle_Receiving "
                                "JOIN Vehicle_Release "
                                "ON [Vehicle_Receiving].Property_Number = Vehicle_Release.Property_Number "
                                "WHERE Release_Date_Time < '12/31/1900 12:00:00 AM'")
        else:
            self.cursor.execute("SELECT Pickup_Code FROM Vehicle_Receiving")

        # Pull results out of row types
        res = [re.sub("[^0-9]", "", str(i[0])) for i in self.cursor.fetchall()]

        return zip(Counter(res).keys(), Counter(res).values())

    def get_oldest_vehicles(self, num=15):
        """
        Gets a list of the vehicles that have been on the lot the longest (oldest receive date, without a valid
        release date)

        :param num: number of rows to return
        :return: list of oldest vehicles
        """
        logging.info("get_oldest_vehicles")
        self.cursor.execute("SELECT TOP {} Vehicle_Release.Property_Number, Receiving_Date_Time, Original_Pickup_Code, "
                            "Pickup_Code_Change_Date, Pickup_Code, Release_Date_Time "
                            "FROM [Vehicle_Release] "
                            "JOIN Vehicle_Receiving "
                            "ON [Vehicle_Receiving].Property_Number = Vehicle_Release.Property_Number "
                            "WHERE Release_Date_Time < '12/31/1900 12:00:00 AM'".format(int(num)))

        return self.cursor.fetchall()

    @staticmethod
    def _is_date_zero(date):
        """
        If the date is stored as a pre-1900 date, then its really just a 'null' date

        :param date: datetime.date to check for nullness
        :return: bool - true if the date is 'null'
        """
        return date < datetime.date(1900, 12, 31)

    @staticmethod
    def _get_valid_filename(filename):
        """
        Retry logic to get a non locked file. A number will be added to the return if its locked

        :param filename: The preferred filename
        :return filename: Valid filename. Not threadsafe
        """
        i = ""
        sfilename = filename.split('.')
        while True:
            filename = "{name}{int}.{ext}".format(name='.'.join(sfilename[0:-1]), int=i, ext=sfilename[-1])
            try:
                with FileLock(filename, timeout=0):
                    break
            except Timeout:
                i = i + 1 if i != "" else 0  # when the file is still open, lets just write it elsewhere
            except FileNotFoundError:
                break
        return filename

    def _process_events(self, receive_date, release_date, code, days_offset=0):
        """
        Increments the number and age of cars for the specified code between the two dates.

        :param receive_date: (Datetime.date) First date (inclusive) when the vehicle was on the lot
        :param release_date: (Datetime.date) End date (inclusive) when the vehicle was on the lot as that code
        :param code: (str) The tow code for the vehicle
        :param days_offset: (int) Number of days the vehicle was on the lot before this event. Useful if the vehicle
        moves from one codetype to another and we want to count the existing age of the vehicle
        :return: none
        """
        logging.debug("_process_events(%s, %s, %s, %s)", receive_date, release_date, code, days_offset)
        if not code:
            category = "nocode"
        else:
            base_code = re.sub("[^0-9]", "", str(code))
            if base_code and int(base_code) in TOW_CATEGORIES.keys():
                category = TOW_CATEGORIES[int(base_code)]
            else:
                # this is garbage data we will use verbatim
                category = "nocode"

        if self._is_date_zero(release_date):
            release_date = datetime.date.today()
        delta = release_date - receive_date

        # For every date, we calculate the number of cars on the lot, and the average age of the cars. Its
        # stored in a hash of date: DataAccumulator
        for i in range(0, delta.days + 1):
            key = receive_date + datetime.timedelta(days=i)

            if receive_date and (receive_date <= key <= release_date):
                num = "{}_num".format(category)
                setattr(self.date_hash[key], num, getattr(self.date_hash[key], num) + 1)
                self.date_hash[key].total_num += 1

                age = "{}_age".format(category)
                setattr(self.date_hash[key], age, getattr(self.date_hash[key], age) + i + days_offset + 1)
                self.date_hash[key].total_age += i + days_offset + 1

    def calculate_vehicle_stats(self, vehicle_rows=None):
        """
        Calculates the number of vehicles and the average age of the vehicles on a per day basis by pulling each
        row and iterating over the data by day

        :param vehicle_rows: (list of lists) Unordered rows of vehicle information in the format [[Property_Number,
        Receiving_Date_Time, Release_Date_Time, Pickup_Code, Pickup_Code_Change_Date, Original_Pickup_Code], ...]
        :return: none
        """
        if vehicle_rows is None:
            vehicle_rows = self.get_all_vehicles()
        # We have to get everything at once because the database doesn't support multiple concurrent connections, and
        # we have other queries. This pulls every single vehicle from the database
        for row in vehicle_rows:
            # Get receive date
            receive_date = self.get_receive_date(row[0]) if self._is_date_zero(row[1].date()) else row[1].date()
            if self._is_date_zero(receive_date):
                logging.debug("Problematic data (receive) %s", row)
                continue

            # Get release date
            release_date = self.get_release_date(row[0]) if self._is_date_zero(row[2].date()) else row[2].date()

            # This means its probably still in the lot, so lets calculate using today as the end date
            if self._is_date_zero(release_date):
                release_date = datetime.date.today()

            logging.info(row[0])
            if not self._is_date_zero(row[4].date()):
                self._process_events(receive_date, row[4].date() - datetime.timedelta(days=1), row[5])
                initial_age = (row[4].date() - receive_date).days
                self._process_events(row[4].date(), release_date, row[3], initial_age)
            else:
                self._process_events(receive_date, release_date, row[3])

    def write_towing(self, filename="towing.csv"):
        """
        Writes the csv file with the date based age and quantity of cars

        :param filename: Filename to output into
        :return: none
        """
        logging.info("write_towing")
        if len(self.date_hash) == 0:
            self.calculate_vehicle_stats()

        csv_columns = ['datetime'] + [f(x) for x in TOW_CATEGORIES.values() for f in (self._app_num, self._app_avg)]
        csv_file = self._get_valid_filename(filename)
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()

            for date, data_acc in sorted(self.date_hash.items()):
                row = {}
                for col in csv_columns:
                    if col == 'datetime':
                        row['datetime'] = date
                    elif '_avg' in col:
                        temp_col = col.replace('_avg', '')  # get base category name
                        vdays = getattr(data_acc, '{}_age'.format(temp_col))
                        vnum = getattr(data_acc, '{}_num'.format(temp_col))
                        row[col] = (vdays / vnum) if vnum > 0 else 0
                    else:
                        row[col] = getattr(data_acc, col)
                writer.writerow(row)

    def write_pickups(self, pickup_types=None, filename="pickups.csv"):
        """
        Writes the csv file with the pickup types and the breakdown

        :param pickup_types: A dictionary of pickup types and quantity, mainly for testing support
        :param filename: Filename to output into
        :return: none
        """
        logging.info("write_pickups")
        if pickup_types is None:
            pickup_types = self.get_pickup_types()
        csv_columns = ['pickup_type', 'base_pickup_type', 'quantity']
        csv_file = self._get_valid_filename(filename)
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            nocode_num = 0
            for pickup_type, quantity in pickup_types:
                if pickup_type == 1000:
                    nocode_num += quantity
                    continue
                try:
                    valid_pickup_type = TOW_CATEGORIES[int(pickup_type)]
                except ValueError:
                    nocode_num += quantity
                    continue
                writer.writerow({'pickup_type': valid_pickup_type,
                                 'base_pickup_type': re.sub("[^0-9]", "", str(pickup_type)),
                                 'quantity': int(quantity)})
            if nocode_num:
                writer.writerow({'pickup_type': 'nocode',
                                 'base_pickup_type': 1000,
                                 'quantity': nocode_num})

    def write_oldest_vehicles(self, vehicles=None, filename="top_15.csv"):
        """
        Writes the csv file with the 15 oldest vehicles on the lot

        :param vehicles: (list) List of vehicles to write to the CSV (mainly for testing support)
        :param filename: Filename to output into
        :return: none
        """
        logging.info("write_oldest_vehicles")
        if vehicles is None:
            vehicles = self.get_oldest_vehicles()

        csv_columns = ['Property_Number', 'Receiving_Date_Time', 'Original_Pickup_Code', 'Pickup_Code_Change_Date',
                       'Pickup_Code', 'Release_Date_Time']
        csv_file = self._get_valid_filename(filename)
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for row in vehicles:
                writer.writerow({'Property_Number': row[0],
                                 'Receiving_Date_Time': row[1],
                                 'Original_Pickup_Code': row[2],
                                 'Pickup_Code_Change_Date': row[3],
                                 'Pickup_Code': row[4],
                                 'Release_Date_Time': row[5]
                                 })

    @staticmethod
    def _app_num(field):
        """Helper for generating dynamic field names"""
        return "{}_num".format(field)

    @staticmethod
    def _app_age(field):
        """Helper for generating dynamic field names"""
        return "{}_age".format(field)

    @staticmethod
    def _app_avg(field):
        """Helper for generating dynamic field names"""
        return "{}_avg".format(field)


def main():
    """
    Main function
    """
    parser = argparse.ArgumentParser(description='Towing data generator')
    parser.add_argument('-t', '--towdata', action='store_true',
                        help='Generate towdata with yard quantity and vehicle ages')
    parser.add_argument('-c', '--categories', action='store_true',
                        help='Generate breakdown of current vehicle categories')
    parser.add_argument('-o', '--oldest', action='store_true',
                        help='Generate list of oldest vehicles')

    args = parser.parse_args()

    towdata = TowingData()

    runall = not any([args.towdata, args.categories, args.oldest])
    if args.towdata or runall:
        towdata.write_towing()
    if args.categories or runall:
        towdata.write_pickups()
    if args.oldest or runall:
        towdata.write_oldest_vehicles()


if __name__ == '__main__':
    main()
