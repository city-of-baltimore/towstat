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

CREATE TABLE [dbo].[towstat_agebydate](
    [date] [date],
    [property_id] [varchar](50),
    [vehicle_age] [int],
    [pickupcode] [varchar](50) NULL,
    [dirtbike] [bit] NULL
)
"""

import logging
import re
from collections import defaultdict
from datetime import datetime, date, timedelta

from tqdm import tqdm
import pyodbc
from namedlist import namedlist, FACTORY  # pylint:disable=import-error

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
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

        for sublist in [(x, "{}_db".format(x)) for x in TOW_CATEGORIES.values()]:
            for item in sublist:
                data_categories.append(item)

        DataAccumulator = namedlist('DataAccumulator', data_categories, default=FACTORY(list))  # pylint:disable=invalid-name

        # Uses the form of datetime: DataAccumulator
        self.date_dict = defaultdict(lambda: DataAccumulator())  # pylint:disable=unnecessary-lambda

    def get_vehicle_records(self, start_date=None, end_date=None):
        """
        Get all-time vehicles that were on the lot for the specified dates

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
                                (Release_Date_Time >= Convert(datetime, '{start_date}'))
                                """).format(start_date=start_date)
            else:
                restriction = ("""WHERE
                                (Receiving_Date_Time <= Convert(datetime, '{end_date}'))
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

    @staticmethod
    def _is_date_zero(check_date):
        """
        If the date is stored as a pre-1900 date, then its really just a 'null' date

        :param check_date: (datetime.date) to check for nullness
        :return: (bool) true if the date is 'null'
        """
        return check_date < date(1900, 12, 31)

    def _process_events(self, receive_date, release_date, code, vehicle_type,  # pylint:disable=too-many-arguments
                        property_num, days_offset=0):
        """
        Increments the number and age of cars for the specified code between the two dates.

        :param receive_date: (Datetime.date) First date (inclusive) when the vehicle was on the lot
        :param release_date: (Datetime.date) End date (inclusive) when the vehicle was on the lot as that code
        :param code: (str) The tow code for the vehicle
        :param vehicle_type: (str) The vehicle type from the vehicle_information table
        :param property_num: (str) Property number of the vehicle to process
        :param days_offset: (int) Number of days the vehicle was on the lot before this event. Useful if the vehicle
        moves from one codetype to another and we want to count the existing age of the vehicle
        :return: none
        """
        logging.debug("_process_events(%s, %s, %s, %s, %s, %s)",
                      receive_date, release_date, code, vehicle_type, property_num, days_offset)
        assert days_offset >= 0

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

        for i in range(delta.days + 1):
            date_key = receive_date + timedelta(days=i)
            if receive_date and (receive_date <= date_key <= release_date):
                category_key = "{}_db".format(category) if vehicle_type in DB_TYPES else "{}".format(category)
                getattr(self.date_dict[date_key], category_key).append((i + days_offset + 1, property_num))

    def calculate_vehicle_stats(self, start_date: datetime.date = None, end_date: datetime.date = None):
        """
        Calculates the number of vehicles and the average age of the vehicles on a per day basis by pulling each
        row and iterating over the data by day

        :param start_date: First date to search, inclusive
        :param end_date: Last date to search, inclusive
        :return: none
        """
        vehicle_rows = self.get_vehicle_records(start_date, end_date)
        # We have to get everything at once because the database doesn't support multiple concurrent connections, and
        # we have other queries. This pulls every single vehicle from the database
        # Row has the following data [0: Property_Number, 1: Receiving_Date_Time, 2: Release_Date_Time, 3: Pickup_Code,
        # 4: Pickup_Code_Change_Date, 5: Original_Pickup_Code, 6: Property_Type]
        for row in tqdm(vehicle_rows):
            # Get receive date
            if self._is_date_zero(row[1].date()):
                logging.info("Problematic data with property number %s. Bad start date.", row[1].date())
                continue

            # This means its probably still in the lot, so lets calculate using today as the end date
            release_date = date.today() if self._is_date_zero(row[2].date()) else row[2].date()

            if self._is_date_zero(row[4].date()):
                self._process_events(row[1].date(), release_date, row[3], row[6], row[0])
            else:
                # This means that the pickup code changed, so we should process this as two different date ranges
                self._process_events(row[1].date(), row[4].date() - timedelta(days=1), row[5], row[6], row[0])
                initial_age = (row[4].date() - row[1].date()).days
                self._process_events(row[4].date(), release_date, row[3], row[6], row[0], initial_age)

    def get_vehicle_ages(self, start_date: datetime.date = date(1899, 12, 31), end_date: datetime.date = date.today()):
        """
        Calculates the vehicle ages for each date/pickup type/vehicle type
        :param start_date: Start date of the range of vehicles to pull (inclusive)
        :param end_date: End date of the range of vehicles to pull (inclusive)
        :return: List of date (y-m-d), property id, vehicle age (in days), pickup code on that date, and dirtbike bit
        """
        logging.info("Write towing: Processing %s to %s", start_date.strftime('%Y-%m-%d'),
                     end_date.strftime('%Y-%m-%d'))

        self.calculate_vehicle_stats(start_date, end_date)

        days = (end_date - start_date)
        all_vehicle_ages = []
        for day in range(days.days + 1):
            towyard_date = (start_date + timedelta(days=day))

            for pickupcode in TOW_CATEGORIES.values():
                for dirtbike in [True, False]:
                    vehicle_list = getattr(self.date_dict[towyard_date],
                                           "{}{}".format(pickupcode, '_db' if dirtbike else ''))

                    for vehicle_age, prop_id in vehicle_list:
                        all_vehicle_ages.append((towyard_date.strftime('%Y-%m-%d'), prop_id, vehicle_age, pickupcode,
                                                 dirtbike))
        return all_vehicle_ages

    def write_towing(self, start_date: datetime.date = date(1899, 12, 31), end_date: datetime.date = date.today(),
                     split=False):
        """
        Writes the date that the vehicle entered and left the lot (if applicable). Also generates the quantity and
        average age of the cars at that time

        :param start_date: First date (inclusive) to write to the database
        :param end_date: Last date (inclusive) to write to the database
        :param split: Process by date instead of the whole date range at once. This is good for very large date ranges.
        Default false
        :return: none
        """
        if split:
            days = end_date - start_date
            for day in range(days.days):
                tow_date = start_date + timedelta(days=day)
                self.write_towing(tow_date, tow_date)

        all_vehicle_ages = self.get_vehicle_ages(start_date, end_date)
        self.cursor311.executemany("""
            MERGE [towstat_agebydate] USING (
            VALUES
                (?, ?, ?, ?, ?)
            ) AS vals (date, property_id, vehicle_age, pickupcode, dirtbike)
            ON (towstat_agebydate.date = vals.date AND
                towstat_agebydate.property_id = vals.property_id)
            WHEN MATCHED THEN
                UPDATE SET
                vehicle_age = vals.vehicle_age,
                pickupcode = vals.pickupcode,
                dirtbike = vals.dirtbike
            WHEN NOT MATCHED THEN
                INSERT (date, property_id, vehicle_age, pickupcode, dirtbike)
                VALUES (vals.date, vals.property_id, vals.vehicle_age, vals.pickupcode, vals.dirtbike);
        """, all_vehicle_ages)
        self.cursor311.commit()
