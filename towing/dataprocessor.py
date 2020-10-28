"""
Preprocesses data from the IVIC towing database, mainly by calculating the number of vehicles on lot, and their average
age per date. This generates a CSV file that is then used by the Shiny towing dashboard.

Relies on the following database structure:

CREATE TABLE [dbo].[towstat_bydate](
    [date] [date] NULL,
    [quantity] [int] NULL,
    [average] [real] NULL,
    [medianage] [real] NULL,
    [dirtbike] [bit] NULL,
    [pickupcode] [varchar](50) NULL
)
"""

import logging
import re
from collections import defaultdict
from datetime import datetime, time, date, timedelta
from statistics import median, mean

import pyodbc
from namedlist import namedlist, FACTORY
from tqdm import tqdm

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

# These are police holds, as opposed to police action, which should be differentiated
POLICE_HOLD = ['111B', '111M', '111N', '111P', '111S', '200P']

# Vehicle types that are not full size vehicles
DB_TYPES = ['DB', 'SCOT', 'ATV']

TOW_CATEGORIES = {
    111: 'police_action',
    1111: 'police_hold',  # not a code; its how we differentiate police_action vs police_hold since we strip subcodes
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

        conn311 = pyodbc.connect('Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
        self.cursor311 = conn311.cursor()

        data_categories = []

        for sublist in [(x, "{}_nondb".format(x)) for x in TOW_CATEGORIES.values()]:
            for item in sublist:
                data_categories.append(item)

        DataAccumulator = namedlist('DataAccumulator', data_categories, default=FACTORY(list))

        # Uses the form of datetime: DataAccumulator
        self.date_dict = defaultdict(lambda: DataAccumulator())  # pylint:disable=unnecessary-lambda

    def get_all_vehicles(self, start_date=None, end_date=None):
        """
        Get all-time vehicles from the database

        :param start_date: First date to search, inclusive
        :type start_date: datetime.date
        :param end_date: Last date to search, inclusive
        :type end_date: datetime.date

        :return: All rows from database with vehicle information
        """

        start_date = start_date.strftime("%Y-%m-%d") if start_date else None
        end_date = end_date.strftime("%Y-%m-%d") if end_date else None

        # We want vehicles with the following:
        # Has a defined start and end date
        #     If start or end date is in the range -> counts
        #     If start date is before start range and end date is after end range -> counts
        # Has no end date (still on lot)
        #     If start is before end range
        restriction = ""
        if start_date or end_date:
            if start_date and end_date:
                restriction = ("""WHERE
                                ((Receiving_Date_Time <= Convert(datetime, '{end_date}')) AND
                                (Receiving_Date_Time >= Convert(datetime, '{start_date}')))
                                OR
                                ((Release_Date_Time <= Convert(datetime, '{end_date}')) AND
                                (Release_Date_Time >= Convert(datetime, '{start_date}')))
                                OR
                                ((Receiving_Date_Time <= Convert(datetime, '{start_date}')) AND
                                (Release_Date_Time >= Convert(datetime, '{end_date}')))
                                """).format(end_date=end_date, start_date=start_date)
            elif start_date:
                restriction = ("""WHERE
                                (Receiving_Date_Time >= Convert(datetime, '{start_date}'))
                                OR
                                (Release_Date_Time >= Convert(datetime, '{start_date}'))
                                OR
                                (Receiving_Date_Time <= Convert(datetime, '{start_date}'))
                                """).format(start_date=start_date)
            else:
                restriction = ("""WHERE
                                (Receiving_Date_Time <= Convert(datetime, '{end_date}'))
                                OR
                                (Release_Date_Time <= Convert(datetime, '{end_date}'))
                                OR
                                (Release_Date_Time >= Convert(datetime, '{end_date}'))
                                """).format(end_date=end_date)

        logging.info("Get_all_vehicles")
        self.cursor.execute(
            """SELECT * FROM
            (
            SELECT Vehicle_Release.Property_Number, Receiving_Date_Time,
                convert(datetime,
                        Replace(Release_Date_Time, Convert(datetime, '1899-12-31 00:00:00.000'), GETDATE()
                        )
                ) as Release_Date_Time,
                Pickup_Code, Pickup_Code_Change_Date, Original_Pickup_Code, Property_Type
            FROM [Vehicle_Release]
            JOIN Vehicle_Receiving
            ON [Vehicle_Receiving].Property_Number = Vehicle_Release.Property_Number
            JOIN Vehicle_Identification
            ON [Vehicle_Receiving].Property_Number = Vehicle_Identification.Property_Number
            ) as innertable
            {restriction}""".format(restriction=restriction))
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

    @staticmethod
    def _is_date_zero(check_date):
        """
        If the date is stored as a pre-1900 date, then its really just a 'null' date

        :param check_date: (datetime.date) to check for nullness
        :return: (bool) true if the date is 'null'
        """
        return check_date < date(1900, 12, 31)

    def _process_events(self, receive_date, release_date, code, vehicle_type,  # pylint:disable=too-many-arguments
                        days_offset=0):
        """
        Increments the number and age of cars for the specified code between the two dates.

        :param receive_date: (Datetime.date) First date (inclusive) when the vehicle was on the lot
        :param release_date: (Datetime.date) End date (inclusive) when the vehicle was on the lot as that code
        :param code: (str) The tow code for the vehicle
        :param vehicle_type: (str) The vehicle type from the vehicle_information table
        :param days_offset: (int) Number of days the vehicle was on the lot before this event. Useful if the vehicle
        moves from one codetype to another and we want to count the existing age of the vehicle
        :return: none
        """
        logging.debug("_process_events(%s, %s, %s, %s, %s)",
                      receive_date, release_date, code, vehicle_type, days_offset)
        if not code:
            # Handle empty codes
            category = "nocode"
        else:
            if str(code) in POLICE_HOLD:
                # Treat this as a separate category
                category = TOW_CATEGORIES[1111]
            else:
                # Strip the letters off the end to merge everything into the major categories
                base_code = re.sub("[^0-9]", "", str(code))
                if base_code and int(base_code) in TOW_CATEGORIES.keys():
                    category = TOW_CATEGORIES[int(base_code)]
                else:
                    # this is garbage data we will use verbatim
                    category = "nocode"

        if self._is_date_zero(release_date):
            release_date = date.today()
        delta = release_date - receive_date

        # For every date, we calculate the number of cars on the lot, and the average age of the cars. Its
        # stored in a hash of date: DataAccumulator

        for i in range(0, delta.days + 1):
            date_key = receive_date + timedelta(days=i)

            if receive_date and (receive_date <= date_key <= release_date):
                if vehicle_type not in DB_TYPES:
                    category_key = "{}_nondb".format(category)
                    getattr(self.date_dict[date_key], category_key).append(i + days_offset + 1)
                else:
                    category_key = "{}".format(category)
                    getattr(self.date_dict[date_key], category_key, []).append(i + days_offset + 1)

    def calculate_vehicle_stats(self, start_date=None, end_date=None):
        """
        Calculates the number of vehicles and the average age of the vehicles on a per day basis by pulling each
        row and iterating over the data by day

        :param start_date: First date to search, inclusive
        :type start_date: datetime.date
        :param end_date: Last date to search, inclusive
        :type end_date: datetime.date
        :return: none
        """

        vehicle_rows = self.get_all_vehicles(start_date, end_date)
        # We have to get everything at once because the database doesn't support multiple concurrent connections, and
        # we have other queries. This pulls every single vehicle from the database
        for row in tqdm(vehicle_rows):
            # Get receive date
            receive_date = self.get_receive_date(row[0]) if self._is_date_zero(row[1].date()) else row[1].date()
            if self._is_date_zero(receive_date):
                logging.debug("Problematic data (receive) %s", row)
                continue

            # Get release date
            release_date = self.get_release_date(row[0]) if self._is_date_zero(row[2].date()) else row[2].date()

            # This means its probably still in the lot, so lets calculate using today as the end date
            if self._is_date_zero(release_date):
                release_date = date.today()

            logging.debug(row[0])
            if not self._is_date_zero(row[4].date()):
                # This means that the pickup code changed, so we should process this as two different date ranges
                self._process_events(receive_date, row[4].date() - timedelta(days=1), row[5], row[6])
                initial_age = (row[4].date() - receive_date).days
                self._process_events(row[4].date(), release_date, row[3], row[6], initial_age)
            else:
                self._process_events(receive_date, release_date, row[3], row[6])

    def write_towing(self, start_date=None, end_date=None):
        """
        Writes the date that the vehicle entered and left the lot (if applicable). Also generates the quantity and
        average age of the cars at that time

        :return: none
        """
        if not start_date:
            start_date = date(1899, 12, 31)

        if not end_date:
            end_date = datetime.combine(date.today(), time(23, 59, 59))

        logging.info("write_towing")
        if len(self.date_dict) == 0:
            self.calculate_vehicle_stats(start_date, end_date)

        data = []

        for towyard_date, data_acc in sorted(self.date_dict.items()):
            towyard_date = datetime.combine(towyard_date, time())
            if not start_date <= towyard_date <= end_date:
                continue

            towyard_date = towyard_date.strftime('%Y-%m-%d')

            all_nondb = []
            all_db = []
            for pickupcode in TOW_CATEGORIES.values():
                for dirtbike_status in ['', '_nondb']:
                    dirtbike = dirtbike_status == ''
                    working_list = getattr(data_acc, "{}{}".format(pickupcode, dirtbike_status))

                    if dirtbike:
                        all_db.extend(working_list)
                    else:
                        all_nondb.extend(working_list)
                    self._calculate_towing_stats(working_list, data, towyard_date, pickupcode, dirtbike)

            # Process the full day totals
            self._calculate_towing_stats(all_nondb, data, towyard_date, 'total', False)
            self._calculate_towing_stats(all_db, data, towyard_date, 'total', True)

        self.cursor311.executemany("""
        MERGE towstat_bydate1 USING (
        VALUES
            (?, ?, ?, ?, ?, ?)
        ) AS vals (date, quantity, average, medianage, dirtbike, pickupcode)
        ON (towstat_bydate1.date = vals.date AND
            towstat_bydate1.dirtbike = vals.dirtbike AND
            towstat_bydate1.pickupcode = vals.pickupcode)
        WHEN MATCHED THEN
            UPDATE SET
            quantity = vals.quantity,
            average = vals.average,
            medianage = vals.medianage
        WHEN NOT MATCHED THEN
            INSERT (date, quantity, average, medianage, dirtbike, pickupcode)
            VALUES (vals.date, vals.quantity, vals.average, vals.medianage, vals.dirtbike, vals.pickupcode);
        """, data)
        self.cursor311.commit()

    @staticmethod
    def _calculate_towing_stats(input_list, output_list, towyard_date, pickupcode, dirtbike_status):
        """
        Calculates the towyard stats by date, when provided the list of vehicle ages by date
        """
        quantity = len(input_list)
        average_age = mean(input_list) if len(input_list) > 0 else 0
        median_age = median(input_list) if len(input_list) > 0 else 0

        logging.debug((towyard_date, quantity, average_age, median_age, dirtbike_status, pickupcode))
        output_list.append((towyard_date, quantity, average_age, median_age, dirtbike_status, pickupcode))
