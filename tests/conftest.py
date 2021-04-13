"""Fixtures for the tests"""
import pytest

from towstat import dataprocessor


@pytest.fixture(name='towingdata')
def fixture_towingdata():
    """ Setup for each test """
    return dataprocessor.TowingData()
