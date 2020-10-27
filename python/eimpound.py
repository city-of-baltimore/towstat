"""Interface with eimpound.com"""
import datetime
import logging
import pickle
import re
import requests

from bs4 import BeautifulSoup
from nameparser import HumanName

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class Eimpound:
    def __init__(self, username, password, retry_period=4):
        """
        :param username: Case sensitive username used to log into eimpound.com
        :param password: Case sensitive password used to log into eimpound.com
        :param retry_period: Number of days between resubmitting VINs. This is to make sure the same VIN isn't submitted
        multiple times while eimpound is searching for the owner information. Resubmission allowed after 24*retry_period
        hours have elapsed
        """
        logging.debug("Creating session for user %s", username)

        self.retry_period = retry_period
        self.session = requests.Session()
        self._login(username, password)

        try:
            self.vin_lookup = pickle.load(open('towdata.pickle', "rb"))
        except (OSError, IOError) as e:
            self.vin_lookup = {}

    def _login(self, username, password):
        data = {'j_username': username,
                'j_password': password}

        self.session.post('https://www.eimpound.com/eimpound/j_spring_security_check', data=data)

    def search_vin(self, vin: str, tow_date: str):
        """
        Pulls the owner information from e-impound
        :param vin: (str) vehicle information number to get the owner information of
        :param tow_date: (str) the date the vehicle was towed, which is stored by eimpound
        :return: (str) Owner name, address
        """
        logging.info("Searching for vin: %s", vin)

        data = {
            'vin': vin,
            'tow_date': tow_date,
            # plate:
            # state:
            # make:
            # model:
            # year:
            # color:
            # inventoryid:
            # reason:
            # custom1:
            # custom2:
            # notes:
            # _released:
            # release_date:
            'search': 'Search'
        }
        resp = self.session.post('https://www.eimpound.com/eimpound/customerData/search', data=data)
        soup = BeautifulSoup(resp.content, "html.parser")
        vehicle_url = soup.find_all('a', href=re.compile(r'/eimpound/customerData/show/'))

        if not vehicle_url:
            logging.warning("%s was not found", vin)
            return None

        resp = self.session.get('https://www.eimpound.com{}'.format(vehicle_url[0]['href']))
        soup = BeautifulSoup(resp.content, "html.parser")
        owner = soup.find_all('li', {'class': 'fieldcontain'}, text=re.compile(r'.*,.*'))
        name, address = owner.split(',')
        name = HumanName(name).as_dict()
        return {'address': address}.update(name)

    def submit_vin(self, vin, tow_date, force=False):
        """
        Submits the VIN to eimpound, so they can look up owner information asyncronously
        :param vin: (str) vehicle indentification number
        :param tow_date: (str) date of tow
        :param force: (bool) queries the VIN, regardless of the retry_period
        :return:
        """
        logging.info("Submitting vin %s", vin)

        if self.vin_lookup.get(vin) and (datetime.datetime.now() - self.vin_lookup[vin]).days < self.retry_period:
            logging.warning('VIN not submitted. It was last submitted on %s, which is less than the retry period (%s)',
                            self.vin_lookup[vin],
                            self.retry_period)
            return False

        data = {
            'vin': vin,
            'tow_date': tow_date,
            # plate:
            # state:
            # make:
            # model:
            # year:
            # color:
            # inventoryid:
            # reason:
            # custom1:
            # custom2:
            # notes:
            # _released:
            # release_date:
            'create': 'Create'
        }

        resp = self.session.post('https://www.eimpound.com/eimpound/customerData/save', data=data)

        if resp.status_code != 200:
            logging.error("Received response code %s", resp.status_code)
            return False

        self.vin_lookup[vin] = datetime.datetime.now()

        return True
