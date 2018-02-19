#!/usr/bin/env python
# encoding: utf-8

import sys
import traceback
import calendar
import logging
from calendar import monthrange
from time import sleep

from flo.ui import safe_submit_order
from timeutil import TimeInterval, datetime, timedelta

import flo.sw.hirs_avhrr as hirs_avhrr
from flo.sw.hirs.utils import setup_logging

# every module should have a LOG object
LOG = logging.getLogger(__name__)

setup_logging(2)

# General information
hirs_version = 'v20151014'
collo_version = 'v20151014'
wedge = timedelta(seconds=1.)
day = timedelta(days=1.)

# Satellite specific information

#satellite = 'noaa-19'
#granule = datetime(2015, 4, 17, 0, 20)
#intervals = [TimeInterval(granule, granule + wedge - wedge)]
#intervals = []
#years = 2009
#intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(4,13) ]
#for years in range(2010, 2018):
    #intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(1,13) ]
#years = 2018
#intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(1,2) ]

satellite = 'metop-b'
#granule = datetime(2017, 6, 25, 0)
#granule_end = datetime(2017, 7, 1, 0)
#intervals = [TimeInterval(granule, granule_end-wedge)]
#intervals = [TimeInterval(granule, granule+day-wedge)]
intervals = []
#years = 2016
#intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(9,13) ]
years = 2017
intervals += [TimeInterval(datetime(years,month,1), datetime(years,month,calendar.monthrange(years,month)[1])+day-wedge) for month in range(7,13) ]


# Data locations for each file type
collection = {'HIR1B': 'ILIAD',
              'CFSR': 'DELTA', # obsolete
              'PTMSX': 'ILIAD'}

# NOAA-19
#input_data = {'HIR1B': '/mnt/sdata/geoffc/HIRS_processing/data_lists/NOAA-19/HIR1B_noaa-19_latest',
              #'CFSR':  '/mnt/sdata/geoffc/HIRS_processing/data_lists/CFSR.out',
              #'PTMSX': '/mnt/sdata/geoffc/HIRS_processing/data_lists/NOAA-19/PTMSX_noaa-19_latest'}

# Metop-B
input_data = {'HIR1B': '/mnt/sdata/geoffc/HIRS_processing/data_lists/Metop-B/HIR1B_metop-b_latest',
              'CFSR':  '/mnt/sdata/geoffc/HIRS_processing/data_lists/CFSR.out',
              'PTMSX': '/mnt/sdata/geoffc/HIRS_processing/data_lists/Metop-B/PTMSX_metop-b_latest'}

input_sources = {'collection':collection, 'input_data':input_data}

# Initialize the hirs module with the data locations
hirs_avhrr.set_input_sources(input_sources)

# Instantiate the computation
comp = hirs_avhrr.HIRS_AVHRR()

satellite_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    'noaa-19', 'metop-a', 'metop-b']

LOG.info("Submitting intervals...")

dt = datetime.utcnow()
log_name = 'hirs_avhrr_{}_s{}_e{}_c{}.log'.format(
    satellite,
    intervals[0].left.strftime('%Y%m%d%H%M'),
    intervals[-1].right.strftime('%Y%m%d%H%M'),
    dt.strftime('%Y%m%d%H%M%S'))

try:

    for interval in intervals:
        LOG.info("Submitting interval {} -> {}".format(interval.left, interval.right))

        contexts =  comp.find_contexts(interval, satellite, hirs_version, collo_version)

        LOG.info("Opening log file {}".format(log_name))
        file_obj = open(log_name,'a')

        LOG.info("\tThere are {} contexts in this interval".format(len(contexts)))
        contexts.sort()

        if contexts != []:
            #for context in contexts:
                #LOG.info(context)

            LOG.info("\tFirst context: {}".format(contexts[0]))
            LOG.info("\tLast context:  {}".format(contexts[-1]))

            try:
                job_nums = []
                job_nums = safe_submit_order(comp, [comp.dataset('out')], contexts, download_onlies=[])

                if job_nums != []:
                    #job_nums = range(len(contexts))
                    #LOG.info("\t{}".format(job_nums))

                    file_obj.write("contexts: [{}, {}]; job numbers: {{{}..{}}}\n".format(contexts[0], contexts[-1], job_nums[0],job_nums[-1]))
                    LOG.info("contexts: [{}, {}]; job numbers: {{{},{}}}".format(contexts[0], contexts[-1], job_nums[0],job_nums[-1]))
                    LOG.info("job numbers: {{{}..{}}}\n".format(job_nums[0],job_nums[-1]))
                else:
                    LOG.info("contexts: {{{}, {}}}; --> no jobs".format(contexts[0], contexts[-1]))
                    file_obj.write("contexts: {{{}, {}}}; --> no jobs\n".format(contexts[0], contexts[-1]))
            except Exception:
                LOG.warning(traceback.format_exc())

            #sleep(30.)

        LOG.info("Closing log file {}".format(log_name))
        file_obj.close()

except Exception:
    LOG.warning(traceback.format_exc())
