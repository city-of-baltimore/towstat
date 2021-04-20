"""Towstat driver script"""
import argparse
import os
import sys
from datetime import date, timedelta
from loguru import logger

from towstat.dataprocessor import TowingData

handlers = [
    {'sink': sys.stdout, 'format': '{time} - {message}', 'colorize': True, 'backtrace': True, 'diagnose': True},
    {'sink': os.path.join('logs', 'file-{time}.log'), 'colorize': True, 'serialize': True, 'backtrace': True,
     'diagnose': True, 'rotation': '1 week', 'retention': '3 months', 'compression': 'zip'},
]

logger.configure(handlers=handlers)

yesterday = date.today() - timedelta(days=1)
parser = argparse.ArgumentParser(description='Tow data parser')
parser.add_argument('-m', '--month', type=int,
                    help='Optional: Month of date we should start searching on (IE: 10 for Oct).')
parser.add_argument('-d', '--day', type=int,
                    help='Optional: Day of date we should start searching on (IE: 5).')
parser.add_argument('-y', '--year', type=int,
                    help='Optional: Year of date we should start searching on (IE: 2020).')
parser.add_argument('-n', '--numofdays', type=int,
                    help='Optional: Number of days to search, including the start date.')
parser.add_argument('-f', '--force', action='store_true',
                    help='Regenerate the data for the date range. By default, it skips dates with existing data.')

args = parser.parse_args()

towdata = TowingData()
if args.year and args.month and args.day and args.numofdays:
    start_date = date(args.year, args.month, args.day)
    towdata.write_towing(start_date=start_date,
                         end_date=start_date + timedelta(days=args.numofdays - 1),
                         force=args.force)
elif args.year or args.month or args.day or args.numofdays:
    logger.critical('If you specify a year/month/day/numofdays, then you must specify them all.')
else:
    start_date = date(2000, 1, 1)
    end_date = date.today() - timedelta(days=1)
    towdata.write_towing(start_date=start_date, end_date=end_date, force=args.force)
