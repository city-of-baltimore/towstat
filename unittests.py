from datetime import datetime, date, timedelta
import csv
import os
import tempfile
import unittest

import towing

class TestTowingData(unittest.TestCase):
    """
    Unit tests for towing.TowingData
    """
    def setUp(self):
        self.towingdata = towing.TowingData()

    def test_get_all_vehicles(self):
        vehicles = self.towingdata.get_all_vehicles()
        self.assertGreater(len(vehicles), 400000)

    def test_get_receive_date(self):
        self.assertEqual(self.towingdata.get_receive_date('P538997'), date(1998, 8, 26))
        self.assertEqual(self.towingdata.get_receive_date('P473548'), date(2002, 6, 24))
        self.assertEqual(self.towingdata.get_receive_date('P311487'), date(2016, 1, 30))

    def test_get_release_date(self):
        self.assertEqual(self.towingdata.get_release_date('P288581'), date(2014, 12, 19))
        self.assertEqual(self.towingdata.get_release_date('P240453'), date(2014, 12, 10))
        self.assertEqual(self.towingdata.get_release_date('P364054'), date(1899, 12, 31))

    def test_get_pickup_types(self):
        pickup_types = self.towingdata.get_pickup_types()

        # Validate that the codes we get back are valid
        verification_pickup_types = ['', '111', '111B', '111N', '112', '111A', '111P', '112D', '112P', '113', '111S',
                                     '112H', '113C', '111D', '140', '200', '125', 'REL', '200P', '111V', '200B', 'RAVE',
                                     '200V', '300']
        for pickup_type, _ in self.towingdata.get_pickup_types():
            self.assertTrue(str(pickup_type) in verification_pickup_types,
                            "Failed on {} not in {}.".format(pickup_type, verification_pickup_types))
            verification_pickup_types.remove(pickup_type)

        test_data = [('111', 100), ('200', 88), ('', 13), ('Bad', 99)]
        validation_data = {'police_action': ['111', '100'],
                           'stolen_recovered': ['200', '88'],
                           'nocode': ['1000', '112']}

        path = os.path.join(tempfile.mkdtemp(), 'pickup_types.csv')
        self.towingdata.write_pickups(test_data, path)
        with open(path) as csv_file:
            actual_data = csv.DictReader(csv_file)
            for actual_row in actual_data:
                self.assertEqual(validation_data[actual_row['pickup_type']][0], actual_row['base_pickup_type'])
                self.assertEqual(validation_data[actual_row['pickup_type']][1], actual_row['quantity'])

        self.assertGreater(os.stat(path).st_size, 100)

    def test_get_oldest_vehicles(self):
        vehicles = self.towingdata.get_oldest_vehicles(100)
        self.assertEqual(len(vehicles), 100)

        vehicles = self.towingdata.get_oldest_vehicles()
        self.assertEqual(len(vehicles), 15)
        for vehicle in vehicles:
            # Lets make sure that every one of the oldest vehicles is over 180 days old. This is assuming that will
            #
            self.assertGreater((date.today() - vehicle[1].date()).days, 180)
            self.assertEqual(vehicle[5].date(), date(1899, 12, 31))

        path = os.path.join(tempfile.mkdtemp(), 'top15.csv')
        self.towingdata.write_oldest_vehicles(vehicles, path)
        self.assertGreater(os.stat(path).st_size, 1200)

    def test_is_date_zero(self):
        self.assertTrue(self.towingdata._is_date_zero(date(1899, 12, 31)))
        self.assertFalse(self.towingdata._is_date_zero(date(1990, 12, 31)))
        self.assertFalse(self.towingdata._is_date_zero(date(2020, 1, 31)))

    def test_get_valid_filename(self):
        import tempfile
        tf = tempfile.mkstemp('.csv')[1]
        self.assertEqual(self.towingdata._get_valid_filename(tf), tf)

        from filelock import FileLock
        with FileLock(tf):
            tfsplit = tf.split('.')
            self.assertEqual(self.towingdata._get_valid_filename(tf), "{}0.{}".format('.'.join(tfsplit[0:-1]),
                                                                                      tfsplit[-1]))

        self.assertEqual(self.towingdata._get_valid_filename(tf), tf)

    def test_process_events(self):
        def daterange(start_date, end_date):
            """
            Helper for iterating over a date range
            """
            for n in range(int ((end_date - start_date).days + 1)):
                yield start_date + timedelta(n)

        validation_data = {
            date(2019, 7, 28): (1, 1),
            date(2019, 7, 29): (2, 3),
            date(2019, 7, 30): (2, 5),
            date(2019, 7, 31): (2, 7),
            date(2019, 8, 1): (2, 9),
            date(2019, 8, 2): (3, 12),
            date(2019, 8, 3): (3, 15),
            date(2019, 8, 4): (3, 18),
            date(2019, 8, 5): (4, 22),
            date(2019, 8, 6): (3, 17),
            date(2019, 8, 7): (3, 20),
            date(2019, 8, 8): (3, 23),
            date(2019, 8, 9): (1, 13),
            date(2019, 8, 10): (1, 14),
            date(2019, 8, 11): (1, 15),
            date(2019, 8, 12): (1, 16),
            date(2019, 8, 13): (1, 17)
        }

        # We populate both the police_action vehicles and the impound with the same data to verify that one does not
        # impact the other, and then we validate that the total values are properly effected.
        self.towingdata._process_events(date(2019, 7, 28), date.today(), '140')
        self.towingdata._process_events(date(2019, 8, 5), date(2019, 8, 8), '140')
        self.towingdata._process_events(date(2019, 8, 2), date(2019, 8, 8), '140')
        self.towingdata._process_events(date(2019, 7, 29), date(2019, 8, 5), '140')

        self.towingdata._process_events(date(2019, 7, 28), date.today(), '111A') # Letters should be dropped
        self.towingdata._process_events(date(2019, 8, 5), date(2019, 8, 8), '111')
        self.towingdata._process_events(date(2019, 8, 2), date(2019, 8, 8), '111N')
        self.towingdata._process_events(date(2019, 7, 29), date(2019, 8, 5), '111')

        self.towingdata._process_events(date(2019, 7, 28), date.today(), '')
        self.towingdata._process_events(date(2019, 8, 5), date(2019, 8, 8), '')
        self.towingdata._process_events(date(2019, 8, 2), date(2019, 8, 8), '')
        self.towingdata._process_events(date(2019, 7, 29), date(2019, 8, 5), '')

        # Trash data
        self.towingdata._process_events(date(2019, 7, 28), date.today(), 'XX')
        self.towingdata._process_events(date(2019, 8, 5), date(2019, 8, 8), 'XX')
        self.towingdata._process_events(date(2019, 8, 2), date(2019, 8, 8), 'XX')
        self.towingdata._process_events(date(2019, 7, 29), date(2019, 8, 5), 'XX')

        delta = (date.today() - date(2019, 7, 28)).days + 1
        self.assertEqual(len(self.towingdata.date_hash), delta)

        for i in daterange(date(2019, 7, 28), date(2019, 8, 13)):
            self.assertEqual(self.towingdata.date_hash[i].impound_num, validation_data[i][0])
            self.assertEqual(self.towingdata.date_hash[i].impound_age, validation_data[i][1])
            self.assertEqual(self.towingdata.date_hash[i].police_action_num, validation_data[i][0])
            self.assertEqual(self.towingdata.date_hash[i].police_action_age, validation_data[i][1])
            self.assertEqual(self.towingdata.date_hash[i].nocode_num, validation_data[i][0] * 2)
            self.assertEqual(self.towingdata.date_hash[i].nocode_age, validation_data[i][1] * 2)
            self.assertEqual(self.towingdata.date_hash[i].total_num, validation_data[i][0] * 4)
            self.assertEqual(self.towingdata.date_hash[i].total_age, validation_data[i][1] * 4)

    def test_calculate_vehicle_stats(self):
        #Property_Number, Receiving_Date_Time, Release_Date_Time, Pickup_Code, Pickup_Code_Change_Date, Orig_Pickup_Code
        test_data = [
            # The codes with a letter get collapsed into their main code
            ['P493459', datetime(2003, 1, 20), datetime(2003, 2, 2), '111A', datetime(1899, 12, 31), '111A'],
            ['P495580', datetime(2003, 2, 2), datetime(2003, 2, 8), '111', datetime(1899, 12, 31), '111'],
            ['P496003', datetime(2003, 2, 7), datetime(2003, 2, 8), '112', datetime(1899, 12, 31), '112'],
            # In the following case, the age does not reset when it gets a new code
            ['P491697', datetime(2003, 1, 20), datetime(2003, 2, 8), '111', datetime(2003, 1, 26), '200'],
            ['P495370', datetime(2003, 1, 31), datetime(2003, 2, 8), '111N', datetime(1899, 12, 31), '111N'],
            ['P494312', datetime(2003, 1, 20), datetime(2003, 2, 5), '200', datetime(2003, 1, 25), '111'],
            ['P494700', datetime(2003, 1, 24), datetime(2003, 2, 2), '111', datetime(1899, 12, 31), '111'],
            ['P495595', datetime(2003, 2, 2), datetime(2003, 2, 7), '112', datetime(1899, 12, 31), '112'],
            ['P495861', datetime(2003, 2, 5), datetime(2003, 2, 7), '111', datetime(2003, 2, 6), '111'],
            ['P495856', datetime(2003, 2, 5), datetime(2003, 2, 7), '111A', datetime(1899, 12, 31), '111A'],
            ['P495973', datetime(2003, 2, 4), datetime(2003, 2, 7), '140', datetime(1899, 12, 31), '140'],
            ['P494901', datetime(2003, 1, 26), datetime(2003, 2, 7), '200', datetime(2003, 2, 6), '112'],
            ['P495903', datetime(2003, 2, 6), datetime(2003, 2, 7), '113', datetime(1899, 12, 31), '113'],
            ['P495978', datetime(2003, 2, 7), datetime(2003, 2, 7), '111', datetime(1899, 12, 31), '111'],
            ['P495979', datetime(2003, 2, 2), datetime(2003, 2, 7), '', datetime(2003, 2, 5), 'XX'],
        ]

        validation_data = {    # total   111     112     113     140     200  nocode(each with _num, _age)
            date(2003, 1, 20): ((3, 3), (2, 2), (0, 0), (0, 0), (0, 0), (1, 1), (0, 0)),
            date(2003, 1, 21): ((3, 6), (2, 4), (0, 0), (0, 0), (0, 0), (1, 2), (0, 0)),
            date(2003, 1, 22): ((3, 9), (2, 6), (0, 0), (0, 0), (0, 0), (1, 3), (0, 0)),
            date(2003, 1, 23): ((3, 12), (2, 8), (0, 0), (0, 0), (0, 0), (1, 4), (0, 0)),
            date(2003, 1, 24): ((4, 16), (3, 11), (0, 0), (0, 0), (0, 0), (1, 5), (0, 0)),
            date(2003, 1, 25): ((4, 20), (2, 8), (0, 0), (0, 0), (0, 0), (2, 12), (0, 0)),
            date(2003, 1, 26): ((5, 25), (3, 17), (1, 1), (0, 0), (0, 0), (1, 7), (0, 0)),
            date(2003, 1, 27): ((5, 30), (3, 20), (1, 2), (0, 0), (0, 0), (1, 8), (0, 0)),
            date(2003, 1, 28): ((5, 35), (3, 23), (1, 3), (0, 0), (0, 0), (1, 9), (0, 0)),
            date(2003, 1, 29): ((5, 40), (3, 26), (1, 4), (0, 0), (0, 0), (1, 10), (0, 0)),
            date(2003, 1, 30): ((5, 45), (3, 29), (1, 5), (0, 0), (0, 0), (1, 11), (0, 0)),
            date(2003, 1, 31): ((6, 51), (4, 33), (1, 6), (0, 0), (0, 0), (1, 12), (0, 0)),
            date(2003, 2, 1): ((6, 57), (4, 37), (1, 7), (0, 0), (0, 0), (1, 13), (0, 0)),
            date(2003, 2, 2): ((9, 66), (5, 42), (2, 9), (0, 0), (0, 0), (1, 14), (1, 1)),
            date(2003, 2, 3): ((7, 49), (3, 21), (2, 11), (0, 0), (0, 0), (1, 15), (1, 2)),
            date(2003, 2, 4): ((8, 57), (3, 24), (2, 13), (0, 0), (1, 1), (1, 16), (1, 3)),
            date(2003, 2, 5): ((10, 67), (5, 29), (2, 15), (0, 0), (1, 2), (1, 17), (1, 4)),
            date(2003, 2, 6): ((10, 60), (5, 34), (1, 5), (1, 1), (1, 3), (1, 12), (1, 5)),
            date(2003, 2, 7): ((12, 72), (6, 40), (2, 7), (1, 2), (1, 4), (1, 13), (1, 6)),
            date(2003, 2, 8): ((4, 38), (3, 36), (1, 2), (0, 0), (0, 0), (0, 0), (0, 0)),
            date(2003, 2, 9): ((0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0), (0, 0))
        }

        self.towingdata.calculate_vehicle_stats(test_data)
        for tow_date, tow_expected in validation_data.items():
            tow_actual = self.towingdata.date_hash[tow_date]
            self.assertEqual(tow_expected[0][0], tow_actual.total_num)
            self.assertEqual(tow_expected[0][1], tow_actual.total_age)
            self.assertEqual(tow_expected[1][0], tow_actual.police_action_num)
            self.assertEqual(tow_expected[1][1], tow_actual.police_action_age)
            self.assertEqual(tow_expected[2][0], tow_actual.accident_num)
            self.assertEqual(tow_expected[2][1], tow_actual.accident_age)
            self.assertEqual(tow_expected[3][0], tow_actual.abandoned_num)
            self.assertEqual(tow_expected[3][1], tow_actual.abandoned_age)
            self.assertEqual(tow_expected[4][0], tow_actual.impound_num)
            self.assertEqual(tow_expected[4][1], tow_actual.impound_age)
            self.assertEqual(tow_expected[5][0], tow_actual.stolen_recovered_num)
            self.assertEqual(tow_expected[5][1], tow_actual.stolen_recovered_age)
            self.assertEqual(tow_expected[6][0], tow_actual.nocode_num)
            self.assertEqual(tow_expected[6][1], tow_actual.nocode_age)

        path = os.path.join(tempfile.mkdtemp(), 'towing.csv')
        self.towingdata.write_towing(path)
        self.assertGreater(os.stat(path).st_size, 1200)

if __name__ == '__main__':
    unittest.main()
