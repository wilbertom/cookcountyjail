from django.core.management.base import BaseCommand
from django.core.exceptions import ObjectDoesNotExist
from countyapi.models import CountyInmate
from scraper.inmate_details import InmateDetails
from scraper.raw_inmate_data import RawInmateData
from datetime import date, timedelta
from functools import partial
import os
import csv
from time import clock

# August 17th, 2013
# see: https://github.com/sc3/cookcountyjail/wiki/Populating-the-Raw-Inmate-Data-from-the-V1.0-API-Database
# this is the as far as we will import data from
EARLIEST_DATA_DAY = date(2013, 8, 17)
ONE_DAY = timedelta(1)
TODAY = date.today()

"""
export CCJ_RAW_INMATE_DATA_RELEASE_DIR=~/Data/ccjexport
export CCJ_RAW_INMATE_DATA_BUILD_DIR=~/Data/ccjexport/build2

export CCJ_RAW_INMATE_DATA_BUILD_DIR=~/Data/ccjexport/build
"""

features = {'CCJ_STORE_RAW_INMATE_DATA': True,
            'CCJ_RAW_INMATE_DATA_RELEASE_DIR': os.environ['CCJ_RAW_INMATE_DATA_RELEASE_DIR'],
            'CCJ_RAW_INMATE_DATA_BUILD_DIR': os.environ['CCJ_RAW_INMATE_DATA_BUILD_DIR']}


class FlattenedInmateDetails(InmateDetails):
    def __init__(self, inmate_record):
        self.record = inmate_record

    def next_court_date(self):
        return None

    def charges(self):
        return None

    def court_house_location(self):
        return None

    def housing_location(self):
        return None

    def age_at_booking(self):
        return self.record.age_at_booking

    def hash_id(self):
        return self.record.person_id

    def bail_amount(self):
        return self.record.bail_amount

    def booking_date(self):
        return self.record.booking_date

    def gender(self):
        return self.record.gender

    def height(self):
        return self.record.height

    def jail_id(self):
        return self.record.jail_id

    def race(self):
        return self.record.race

    def weight(self):
        return self.record.weight


class StateInmateDetails(FlattenedInmateDetails):
    def __init__(self, at_date, inmate_record):
        self.record = inmate_record

        self.at_date = at_date
        self.court_record = None

        self._set_court_record()

    def _set_court_record(self):
        # TODO: assert sorting
        cd = self.record.court_dates.exclude(date__lt=self.at_date)

        if cd.count() > 0:
            self.court_record = cd[0]
        else:
            self.court_record = None

        return self

    def next_court_date(self):
        return None if self.court_record is None else self.court_record.date

    def charges(self):
        # TODO assert sorting
        try:
            return self.record.charges_history.exclude(date_seen__gt=self.at_date).latest('date_seen').charges
        except ObjectDoesNotExist:
            return None

    def court_house_location(self):
        return None if self.court_record is None else self.court_record.location.location.encode('utf-8')

    def housing_location(self):
        try:
            return self.record.housing_history.exclude(housing_date_discovered__gt=self.at_date).latest('housing_date_discovered').housing_location.housing_location
        except ObjectDoesNotExist:
            return None


data_dir = partial(os.path.join, os.environ['CCJ_RAW_INMATE_DATA_BUILD_DIR'])


def dump(date, all_inmates):
    inmates = all_inmates.exclude(booking_date__gt=date).exclude(discharge_date_latest__lt=date)
    data_writer = RawInmateData(date, features, None)

    for inmate in inmates:
        flat_inmate = StateInmateDetails(date, inmate)
        data_writer.add(flat_inmate)

    print("%s, %s, %s" % (date, inmates.count(), clock()))


class Command(BaseCommand):

    def handle(self, *args, **options):
        inmates = CountyInmate.objects.prefetch_related('housing_history', 'court_dates', 'charges_history')\
            .filter(booking_date__range=(EARLIEST_DATA_DAY, TODAY))

        print("Date, Inmate Count, Clock")
        i = EARLIEST_DATA_DAY
        while i < TODAY:
            dump(i, inmates)
            i += ONE_DAY
