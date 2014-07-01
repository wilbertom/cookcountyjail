from django.core.management.base import BaseCommand
from countyapi.models import CountyInmate
from scraper.inmate_details import InmateDetails
from scraper.raw_inmate_data import RawInmateData
from datetime import date, timedelta
import os
from time import clock

# August 17th, 2013
# see: https://github.com/sc3/cookcountyjail/wiki/Populating-the-Raw-Inmate-Data-from-the-V1.0-API-Database
# this is the as far as we will import data from
EARLIEST_DATA_DAY = date(2013, 8, 17)
ONE_DAY = timedelta(1)
TODAY = date.today()
YESTERDAY = TODAY - ONE_DAY


def inmates_in_jail(at_date, inmates):
    """
    Returns the inmates who where in jail
    at the date
    :param at_date:
    :param inmates:
    :return:
    """
    # remove all the inmates who have not being booked yet
    # then remove the ones who were discharged before the date
    return inmates.exclude(
        booking_date__gt=at_date
    ).exclude(
        discharge_date_latest__lt=at_date
    )


def record_at(date, history, key):
    if len(history) == 0:
        return None
    else:
        
        records_sorted = history.exclude(**{'%s__gt' % key: date}).order_by(key)
        return None if len(records_sorted) == 0 else records_sorted[0]


class FlattenedInmateDetails(InmateDetails):

    def __init__(self, state_at_date, inmate_record):
        self.state_at_date = state_at_date
        self.record = inmate_record

        self._court_location = None
        self._court_date = None
        self._housing_location = None
        self._charges = None

        self.flatten(inmate_record)

    def record_at(self, history, k):
        return record_at(self.state_at_date, history.all(), k)

    def flat_housing_location(self, record):
        _housing_location = self.record_at(record.housing_history, 'housing_date_discovered')
        return None if _housing_location is None else _housing_location.housing_location.\
            housing_location

    def flat_charges(self, record):
        _charges = self.record_at(record.charges_history, 'date_seen')
        return None if _charges is None else _charges.charges

    def flat_court(self, record):
        return self.record_at(record.court_dates, 'date')

    def flatten(self, record):
        _court = self.flat_court(record)
        if _court is not None:
            # TODO: remove to go faster, here becuase of record 2013-0817010
            self._court_location = _court.location.location.encode('utf-8')
            self._court_date = _court.date

        self._housing_location = self.flat_housing_location(record)
        self._charges = self.flat_charges(record)

        return self

    def age_at_booking(self):
        return self.record.age_at_booking

    def hash_id(self):
        return self.record.person_id

    def bail_amount(self):
        return self.record.bail_amount

    def booking_date(self):
        return self.record.booking_date

    def charges(self):
        return self._charges

    def court_house_location(self):
        return self._court_location

    def gender(self):
        return self.record.gender

    def height(self):
        return self.record.height

    def housing_location(self):
        return self._housing_location

    def jail_id(self):
        return self.record.jail_id

    def next_court_date(self):
        return self._court_date

    def race(self):
        return self.record.race

    def weight(self):
        return self.record.weight


class Command(BaseCommand):

    def handle(self, *args, **options):
        all_inmates = CountyInmate.objects.filter(booking_date__gte=EARLIEST_DATA_DAY)

        f = {'CCJ_STORE_RAW_INMATE_DATA': True,
             'CCJ_RAW_INMATE_DATA_RELEASE_DIR': os.environ['CCJ_RAW_INMATE_DATA_RELEASE_DIR'],
             'CCJ_RAW_INMATE_DATA_BUILD_DIR': os.environ['CCJ_RAW_INMATE_DATA_BUILD_DIR']}

        current_date = EARLIEST_DATA_DAY

        while current_date <= YESTERDAY:
            print ('**************** On Date: %s - %d ****************' % (current_date, clock()))
            inmates_for_date = inmates_in_jail(current_date, all_inmates)
            data_writer = RawInmateData(current_date, f, None)

            for i in inmates_for_date:
                data_writer.add(FlattenedInmateDetails(current_date, i))

            current_date += ONE_DAY
