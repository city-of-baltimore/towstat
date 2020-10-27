import argparse
import datetime
import logging
import re

import creds
import eimpound
import pyodbc

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

CONN = pyodbc.connect(r'Driver={SQL Server};Server=DOT-FS04-SRV\DOT_FS04;Database=IVIC;Trusted_Connection=yes;')
CONN_test = pyodbc.connect(r'Driver={SQL Server};Server=balt-sql311-prd;Database=DOT_DATA;Trusted_Connection=yes;')
CURSOR = CONN.cursor()
CURSOR_test = CONN_test.cursor()

EIMP = eimpound.Eimpound(creds.EIMPOUND_USER, creds.EIMPOUND_PASS)


def get_ownerless_vehicles():
    """
    Pulls a list of vins/tow dates for vehicles that lack owner information
    :rtype: List
    :return: A list of tuples in the format [(<vin>, <tow_date>, <property number>), ...] with types
    [((str), (datetime) (str)), ...]
    """
    logging.info("Looking up vehicles without ownership information")
    CURSOR.execute("""
        SELECT [Vehicle_Receiving].[Property_Number]
              ,[Vehicle_Identification].[VIN]
              ,[Receiving_Date_Time]
              ,[Vehicle_Release].[Release_Date_Time]
              ,[Owner_Address]
          FROM [Vehicle_Receiving]
          LEFT OUTER JOIN [Vehicle_Owners]
          ON [Vehicle_Receiving].[Property_Number] = [Vehicle_Owners].[Property_Number]
          JOIN [Vehicle_Release]
          ON [Vehicle_Release].Property_Number = [Vehicle_Receiving].Property_Number
          JOIN [Vehicle_Identification]
          ON [Vehicle_Identification].[Property_Number] = [Vehicle_Receiving].[Property_Number]
          WHERE [Owner_Address] IS NULL AND convert(date, Release_Date_Time) < convert(date, '1/1/1990') AND 
                [Vehicle_Identification].[VIN] != ''
    """)
    pattern = re.compile("[A-HJ-NPR-Z0-9]{17}")
    return [(x[1], x[2], x[0]) for x in CURSOR.fetchall() if pattern.match(x[1])]


def pull_owner_information():
    """
    Queries eimpound for owner information
    :return:
    """
    logging.info("Getting ownership information from eimpound")
    data = []
    for vin, tow_date, prop_id in get_ownerless_vehicles():
        owner_info = EIMP.search_vin(vin, tow_date)
        if owner_info is None:
            # We can just resubmit because of the built in retry limiter
            EIMP.submit_vin(vin, tow_date)
            continue

        data.append((prop_id,
                     ' '.join([owner_info['last'], owner_info['suffix']]),
                     ' '.join([owner_info['title']], owner_info['first'], owner_info['middle']),
                     0,  # [Record_Number]
                     owner_info['full_name'],
                     owner_info['address'],
                     None,
                     0,  # [Notification_Required]
                     0,
                     0,  # [Owner_Soundex]
                     0,  # [Owner_Verified]
                     0,  # [Assumed_Owner]
                     0,
                     None,
                     0,
                     ">>Entered:{} script<<".format(datetime.datetime.strftime(datetime.date.today(), '%m/%d/%Y')),
                     0))

    if data:
        CURSOR_test.executemany("""
            MERGE [ivic_vehicle_owners] USING (
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ) AS vals (Property_Number, Last_Name, First_Name, Record_Number, Owner_Name, Owner_Address, Owner_Telephone, 
            Notification_Required, Renotify_If_No_Reply_In_X_Days, Owner_Soundex, Owner_Verified, Assumed_Owner, 
            Send_Another_Letter, Contact_By_Telephone_Made, Send_To_the_Estate_Of, Comments, 
            Owner_Number)
            ON (ivic_vehicle_owners.Property_Number = vals.Property_Number)
            WHEN NOT MATCHED THEN
                INSERT (Property_Number, Last_Name, First_Name, Record_Number, Owner_Name, Owner_Address, Owner_Telephone, 
                Notification_Required, Renotify_If_No_Reply_In_X_Days, Owner_Soundex, Owner_Verified, Assumed_Owner, 
                Send_Another_Letter, Contact_By_Telephone_Made, 
                Send_To_the_Estate_Of, Comments, Owner_Number)
                VALUES (Property_Number, Last_Name, First_Name, Record_Number, Owner_Name, Owner_Address, Owner_Telephone, 
                Notification_Required, Renotify_If_No_Reply_In_X_Days, Owner_Soundex, Owner_Verified, Assumed_Owner, 
                Send_Another_Letter, Contact_By_Telephone_Made, Send_To_the_Estate_Of, Comments, Owner_Number);
            """, data)
        CURSOR_test.commit()


def submit_ownerless_vins():
    """
    Searches the database for all vehicles that do not have ownership information yet
    :return:
    """
    for vin, tow_date in get_ownerless_vehicles():
        EIMP.submit_vin(vin, tow_date)


def start_from_cmd_line():
    """
    Starts the script and processes command line arguments
    :return: None
    """
    parser = argparse.ArgumentParser(description="Submits VIN numbers to EImpounds and queries for completed records")
    parser.add_argument('-s', '--submit', action='store_true', help="Submits ownerless VINs to eimpounds")
    parser.add_argument('-p', '--pull', action='store_true', help='Pulls VINs of previously submitted vehicles')
    args = parser.parse_args()

    if args.pull or not all([args.pull, args.submit]):
        pull_owner_information()

    if args.submit or not all([args.pull, args.submit]):
        submit_ownerless_vins()


if __name__ == '__main__':
    start_from_cmd_line()
