"""
Preprocesses data from the IVIC towing database by calculating the age by day for easier use in PowerBI

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
import os
import re
import sys
from collections import defaultdict
from dataclasses import Field, field, make_dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

from loguru import logger
from tqdm import tqdm  # type: ignore
import pyodbc  # type: ignore


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

data_categories: List[Tuple[str, type, Field]] = []
for sublist in [(x, "{}_db".format(x)) for x in TOW_CATEGORIES.values()]:
    for item in sublist:
        data_categories.append((item, list, field(default_factory=list)))  # type: ignore  # noqa

DataAccumulator = make_dataclass('DataAccumulator', data_categories)


class TowingData:
    """Manages towing database, data processing, and writing files"""

    def __init__(self, towdb_conn_str: Optional[str] = None, db_conn_str: Optional[str] = None):
        if towdb_conn_str is None:
            towdb_conn_str = r'Driver={ODBC Driver 17 for SQL Server};Server=DOT-FS04-SRV\DOT_FS04;Database=IVIC;' \
                             r'Trusted_Connection=yes;'

        if db_conn_str is None:
            db_conn_str = 'Driver={ODBC Driver 17 for SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;' \
                          'Trusted_Connection=yes;'

        conn = pyodbc.connect(towdb_conn_str)  # pylint:disable=c-extension-no-member
        self.cursor = conn.cursor()

        conn311 = pyodbc.connect(db_conn_str)
        self.cursor311 = conn311.cursor()

        # Uses the form of datetime: DataAccumulator
        self.date_dict: Dict[date, Tuple[int, str]] = defaultdict(lambda: DataAccumulator())  # pylint:disable=unnecessary-lambda

    def get_vehicle_records(self, start_date: date = None, end_date: date = None):
        """
        Get all-time vehicles that were on the lot for the specified dates

        :param start_date: First date to search, inclusive
        :param end_date: Last date to search, inclusive

        :return: All rows from database with vehicle information
        """

        # We want vehicles with the following:
        # Has a defined start and end date
        #     If start or end date is in the range -> counts
        #     If start date is before start range and end date is after end range -> counts
        # Has no end date (still on lot)
        #     If start is before end range
        if start_date and end_date:
            assert start_date and end_date
            restriction = ("""WHERE
                            ((Receiving_Date_Time <= Convert(datetime, '{end_date}')) AND
                            (Receiving_Date_Time >= Convert(datetime, '{start_date}')))
                            OR
                            ((Release_Date_Time <= Convert(datetime, '{end_date}')) AND
                            (Release_Date_Time >= Convert(datetime, '{start_date}')))
                            OR
                            ((Receiving_Date_Time <= Convert(datetime, '{start_date}')) AND
                            (Release_Date_Time >= Convert(datetime, '{end_date}')))
                            """).format(end_date=end_date.strftime("%Y-%m-%d"),
                                        start_date=start_date.strftime("%Y-%m-%d"))
        elif start_date:
            assert start_date
            restriction = ("""WHERE
                            (Release_Date_Time >= Convert(datetime, '{start_date}'))
                            """).format(start_date=start_date.strftime("%Y-%m-%d"))
        else:
            assert end_date
            restriction = ("""WHERE
                            (Receiving_Date_Time <= Convert(datetime, '{end_date}'))
                            """).format(end_date=end_date.strftime("%Y-%m-%d"))

        logger.info("Get_all_vehicles")
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
    def _is_date_zero(check_date: date) -> bool:
        """
        If the date is stored as a pre-1900 date, then its really just a 'null' date

        :param check_date: to check for nullness
        :return: true if the date is 'null'
        """
        return check_date < date(1900, 12, 31)

    def _process_events(self, receive_date: date, release_date: date, code: str, vehicle_type: str,  # pylint:disable=too-many-arguments
                        property_num: str, days_offset: int = 0) -> None:
        """
        Increments the number and age of cars for the specified code between the two dates.

        :param receive_date: First date (inclusive) when the vehicle was on the lot
        :param release_date: End date (inclusive) when the vehicle was on the lot as that code
        :param code: The tow code for the vehicle
        :param vehicle_type: The vehicle type from the vehicle_information table
        :param property_num: Property number of the vehicle to process
        :param days_offset: Number of days the vehicle was on the lot before this event. Useful if the vehicle
        moves from one codetype to another and we want to count the existing age of the vehicle
        :return: none
        """
        logger.debug("_process_events({}, {}, {}, {}, {}, {})",
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

    def calculate_vehicle_stats(self, start_date: date = None, end_date: date = None,
                                vehicle_rows: list = None) -> None:
        """
        Calculates the number of vehicles and the average age of the vehicles on a per day basis by pulling each
        row and iterating over the data by day

        :param start_date: First date to search, inclusive. Used if vehicle_rows is not specified.
        :param end_date: Last date to search, inclusive. Used if vehicle_rows is not specified.
        :param vehicle_rows: The rows to process. List in the format [Property_Number, Receiving_Date_Time,
        Release_Date_Time, Pickup_Code, Pickup_Code_Change_Date, Original_Pickup_Code, Property_Type]
        """
        if not vehicle_rows:
            vehicle_rows = self.get_vehicle_records(start_date, end_date)
        # We have to get everything at once because the database doesn't support multiple concurrent connections, and
        # we have other queries. This pulls every single vehicle from the database
        # Row has the following data [Property_Number, Receiving_Date_Time, Release_Date_Time, Pickup_Code,
        # Pickup_Code_Change_Date, Original_Pickup_Code, Property_Type]
        for row in tqdm(vehicle_rows):
            # Get receive date
            if self._is_date_zero(row[1].date()):
                logger.info("Problematic data with property number {}. Bad start date.", row[1].date())
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

    def get_vehicle_ages(self, start_date: date = date(1899, 12, 31),
                         end_date: date = date.today()) -> List[Tuple[str, str, int, str, bool]]:
        """
        Calculates the vehicle ages for each date/pickup type/vehicle type
        :param start_date: Start date of the range of vehicles to pull (inclusive)
        :param end_date: End date of the range of vehicles to pull (inclusive)
        :return: List of date (y-m-d), property id, vehicle age (in days), pickup code on that date, and dirtbike bit
        """
        logger.info("Processing {} to {}", start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

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

    def write_towing(self, start_date: date = date(1899, 12, 31), end_date: date = date.today(), force: bool = False):
        """
        Writes the date that the vehicle entered and left the lot (if applicable). Also generates the quantity and
        average age of the cars at that time

        :param start_date: First date (inclusive) to write to the database
        :param end_date: Last date (inclusive) to write to the database
        :param force: Regenerate the data for the date range. By default, it skips dates with existing data.
        :return: none
        """
        if not force:
            # get populated dates
            self.cursor311.execute("""
                SELECT DISTINCT([date])
                FROM [DOT_DATA].[dbo].[towstat_agebydate]
                WHERE date > convert(date, ?) and date < convert(date, ?)
            """, start_date, end_date)
            actual_dates = {i[0] for i in self.cursor311.fetchall()}
        else:
            actual_dates = set()

        expected_dates = {start_date + timedelta(days=i) for i in range((end_date-start_date).days + 1)}

        for proc_date in expected_dates - actual_dates:
            all_vehicle_ages = self.get_vehicle_ages(proc_date, proc_date)
            if not all_vehicle_ages:
                return
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
