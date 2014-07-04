from django.core.management.base import BaseCommand
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


class InmateDetailsState(FlattenedInmateDetails):
    
    def __init__(self, inmate_record, at_date):
        super(InmateDetailsState, self).__init__(inmate_record)
        self.state_date = at_date

    def next_court_date(self):
        return None

    def charges(self):
        return None

    def court_house_location(self):
        return None

    def housing_location(self):
        return None


def at(date, history):
    records = history.filter(date__range=(date, TODAY)).order_by('date')
    if len(records) == 0:
        return None
    else:
        return records[0]



class Command(BaseCommand):

    def each_day(self, f):

        inmates = self.all_inmates

        i_date = EARLIEST_DATA_DAY

        while i_date < TODAY:
            inmates = inmates.exclude(discharge_date_latest__lte=i_date)
            f(i_date, inmates.exclude(booking_date__gt=i_date))

            i_date += ONE_DAY

    def first_pass(self):
        """
        First pass, about 8 seconds * number of days.

        """

        def write_flat_records(current_date, inmates_at_date):
            print('Processing %s - %d' % (current_date, clock()))
            # permanently remove all the inmates that have been discharged

            data_writer = RawInmateData(current_date, features, None)

            for inmate in inmates_at_date:
                data_writer.add(FlattenedInmateDetails(inmate))

        self.each_day(write_flat_records)

        return self

    def second_pass(self):
        """
        Rewriting the data into the new file takes around one second. The
        issue is in loading the relational data.

        1 second * number of files

        """

        data_dir = partial(os.path.join, os.environ['CCJ_RAW_INMATE_DATA_BUILD_DIR'])

        def add_inmates_relations(current_date, inmates_at_date):
            print('Processing %s - %d' % (current_date, clock()))
            file_path = data_dir(current_date.strftime('%Y-%m-%d'))
            i = csv.DictReader(open("%s.csv" % (file_path), 'r'))
            o = csv.DictWriter(open("%s-%s.csv" % (file_path, 'with-relations'), 'w'), i.fieldnames)
            o.writeheader()

            for row, inmate in zip(i, inmates_at_date):
                # TODO: this is all a lie
                court_date = at(current_date, inmate.court_dates)
                if court_date:
                    row['Court_Date'] = court_date.date
                    row['Court_Location'] = court_date.location.location.encode('utf-8')

                o.writerow(row)

        self.each_day(add_inmates_relations)

        return self

    def query_inmates(self):
        self.all_inmates = CountyInmate.objects.filter(booking_date__gte=EARLIEST_DATA_DAY)
        return self

    def handle(self, *args, **options):
        self.query_inmates()
        # self.first_pass()

        self.all_inmates.prefetch_related('court_dates')
        self.second_pass()

