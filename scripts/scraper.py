#!/usr/bin/env python

#
# This scraps the Cook County Jail website populating the 2.0 API
# system with the results of the scrapping.
#
# This is expected to run after the existing 1.0 API scraper has run,
# so the target date is 11am. If it runs at that time or later in the
# day then it fetches the sumarized date from yesterday, otherwise it
# fetches the info from the day before yesterday.
#

from datetime import datetime, timedelta
from summarize_daily_population_changes import SummarizeDailyPopulationChanges

current_time = datetime.today()
sdpc = SummarizeDailyPopulationChanges()
sdpc.date(str(current_time.date() -
              timedelta(1 if current_time.hour >= 11 else 2)))