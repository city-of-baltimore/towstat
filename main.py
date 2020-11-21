"""Towstat driver script"""
import argparse
from datetime import date, timedelta

from towing.dataprocessor import TowingData

# pylint:disable=invalid-name

yesterday = date.today() - timedelta(days=1)
parser = argparse.ArgumentParser(description='Tow data parser')
parser.add_argument('-m', '--month', type=int, default=yesterday.month,
                    help=('Optional: Month of date we should start searching on (IE: 10 for Oct). Defaults to '
                          'yesterday if not specified'))
parser.add_argument('-d', '--day', type=int, default=yesterday.day,
                    help=('Optional: Day of date we should start searching on (IE: 5). Defaults to yesterday if '
                          'not specified'))
parser.add_argument('-y', '--year', type=int, default=yesterday.year,
                    help=('Optional: Year of date we should start searching on (IE: 2020). Defaults to yesterday '
                          'if not specified'))
parser.add_argument('-n', '--numofdays', default=1, type=int,
                    help='Optional: Number of days to search, including the start date. Defaults to 1 day.')

args = parser.parse_args()

start_date = None
end_date = None

if args.year and args.month and args.day:
    start_date = date(args.year, args.month, args.day)
    end_date = start_date + timedelta(days=args.numofdays - 1)

towdata = TowingData()
towdata.write_towing(start_date=start_date, end_date=end_date)
