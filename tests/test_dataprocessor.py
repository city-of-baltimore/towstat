""" test suite for towstat.dataprocessor """
from datetime import datetime, timedelta, date
import pytest

from towstat import dataprocessor


@pytest.fixture(name='towingdata')
def fixture_towingdata():
    """
    Setup for each test
    """
    return dataprocessor.TowingData()


def _verify_vehicle_rows(row):
    assert len(row[0]) == 7, "Unexpected row length {}".format(row[0])
    assert isinstance(row[0], str)
    assert isinstance(row[1], datetime)
    assert isinstance(row[2], datetime)
    assert isinstance(row[3], str)
    assert isinstance(row[4], datetime)
    assert isinstance(row[5], str)
    assert isinstance(row[6], str)
    return True


def test_get_all_vehicles(towingdata):
    """ tests get_all_vehicles """
    # default dates
    """
    res = towingdata.get_vehicle_records()
    assert len(res) > 425000, "Not enough rows in default date test"
    assert _verify_vehicle_rows(res[0])
    assert _verify_vehicle_rows(res[-1])

    # use set dates on both sides
    start_date = date(2020, 1, 1)
    end_date = date(2020, 1, 2)
    res = towingdata.get_vehicle_records(start_date, end_date)
    invalid_rows = [x for x in res
                    if not ((start_date <= x[2] <= end_date) or
                            (start_date <= x[1] <= end_date) or
                            (x[1] <= start_date and end_date <= x[2]))]
    assert not invalid_rows, "Got invalid rows {}".format(invalid_rows)
    assert 3400 > len(res) > 3300, "Unexpected number of rows: {}".format(len(res))
    assert _verify_vehicle_rows(res[0])
    assert _verify_vehicle_rows(res[-1])

    # use default end date
    start_date = date.today() - timedelta(days=2)
    res = towingdata.get_vehicle_records(start_date)
    assert len(res) < 8000, "Not enough rows in default date test"
    assert _verify_vehicle_rows(res[0])
    assert _verify_vehicle_rows(res[-1])

    # use default start date
    end_date = date(2002, 1, 2)
    res = towingdata.get_vehicle_records(end_date=end_date)
    assert len(res) < 8000, "Not enough rows in default date test"
    assert _verify_vehicle_rows(res[0])
    assert _verify_vehicle_rows(res[-1])
    """
    # check that a one day old vehicle shows up as one day old
    today = datetime.today()
    towingdata.calculate_vehicle_stats(vehicle_rows=[('P1', today, today, '111', datetime(1899, 12, 31), '111',
                                                      'ATV')])
    assert towingdata.date_dict[today.date()].police_action_db[0][0] == 1


def test_is_date_zero(towingdata):
    """ tests _is_date_zero """
    assert towingdata._is_date_zero(date(1899, 12, 31))  # pylint:disable=protected-access
    assert not towingdata._is_date_zero(date(1910, 12, 31))  # pylint:disable=protected-access


def test_calculate_vehicle_stats(towingdata):
    """ tests calculate_vehicle_stats """
    start_date = date(2020, 1, 1)
    end_date = date(2020, 1, 2)

    towingdata.calculate_vehicle_stats(start_date, end_date)
    towingdata.date_dict.keys()


def test_process_events(towingdata):
    """ tests process_events """
    # Test standard event
    start_date = date(2020, 1, 1)
    end_date = date(2020, 2, 1)

    towingdata._process_events(start_date, end_date, '111', 'VAN', 'P1', 0)  # pylint:disable=protected-access
    expected = {end_date - timedelta(days=x) for x in range(32)}
    actual = set(towingdata.date_dict.keys())
    assert not(expected - actual) and not(actual - expected), \
        "Difference between date sets expected: {}\nactual: {}".format(expected, actual)
    assert towingdata.date_dict[start_date].police_action == [(1, 'P1')], "Police action value incorrect"
    assert towingdata.date_dict[end_date].police_action == [(32, 'P1')], "Police action value incorrect"

    for k in ['police_action_db', 'police_hold', 'police_hold_db', 'accident', 'accident_db', 'abandoned',
              'abandoned_db', 'scofflaw', 'scofflaw_db', 'impound', 'impound_db', 'stolen_recovered',
              'stolen_recovered_db', 'commercial_vehicle_restriction', 'commercial_vehicle_restriction_db',
              'nocode', 'nocode_db']:
        assert getattr(towingdata.date_dict[start_date], k) == [], "Unexpected value for key {}".format(k)

    # Test another 111 event with an offset
    towingdata._process_events(start_date, end_date, '111', 'VAN', 'P2', 30)  # pylint:disable=protected-access
    assert towingdata.date_dict[start_date].police_action == [(1, 'P1'), (31, 'P2')], "Police action value incorrect"  # pylint:disable=protected-access
    assert towingdata.date_dict[end_date].police_action == [(32, 'P1'), (62, 'P2')], "Police action value incorrect"  # pylint:disable=protected-access
    for k in ['police_action_db', 'police_hold', 'police_hold_db', 'accident', 'accident_db', 'abandoned',
              'abandoned_db', 'scofflaw', 'scofflaw_db', 'impound', 'impound_db', 'stolen_recovered',
              'stolen_recovered_db', 'commercial_vehicle_restriction', 'commercial_vehicle_restriction_db',
              'nocode', 'nocode_db']:
        assert getattr(towingdata.date_dict[start_date], k) == [], "Unexpected value for key {}".format(k)

    # Test a dirtbike
    towingdata._process_events(start_date, end_date, '111', 'ATV', 'P3', 0)  # pylint:disable=protected-access
    assert towingdata.date_dict[start_date].police_action_db == [(1, 'P3')], "Police action value incorrect"  # pylint:disable=protected-access
    assert towingdata.date_dict[end_date].police_action_db == [(32, 'P3')], "Police action value incorrect"  # pylint:disable=protected-access
    for k in ['police_hold', 'police_hold_db', 'accident', 'accident_db', 'abandoned', 'abandoned_db', 'scofflaw',
              'scofflaw_db', 'impound', 'impound_db', 'stolen_recovered', 'stolen_recovered_db',
              'commercial_vehicle_restriction', 'commercial_vehicle_restriction_db', 'nocode', 'nocode_db']:
        assert getattr(towingdata.date_dict[start_date], k) == [], "Unexpected value for key {}".format(k)


def test_get_vehicle_ages(towingdata):
    """ Tests get_vehicle_age """
    vehicle_ages = towingdata.get_vehicle_ages(date(2020, 1, 1), date(2020, 1, 3))
    assert {x[0] for x in vehicle_ages} == {'2020-01-01', '2020-01-02', '2020-01-03'}
