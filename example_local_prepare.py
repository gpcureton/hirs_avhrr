import os
from datetime import datetime, timedelta

from flo.time import TimeInterval
from flo.ui import local_prepare, local_execute, submit_order
#from flo.sw.hirs import HIRS
from flo.sw.hirs_avhrr import HIRS_AVHRR

import logging
import traceback

# every module should have a LOG object
LOG = logging.getLogger(__file__)

# Set up the logging
levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
level = levels[3]
if level == logging.DEBUG:
    console_logFormat = '%(asctime)s.%(msecs)03d (%(levelname)s) : %(filename)s : %(funcName)s : %(lineno)d:%(message)s'
    dateFormat = '%Y-%m-%d %H:%M:%S'
else:
    console_logFormat = '%(asctime)s.%(msecs)03d (%(levelname)s) : %(message)s'
    dateFormat = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(level=levels[2],
                    format=console_logFormat,
                    datefmt=dateFormat)


# General information
comp = HIRS_AVHRR()

#
# Local execution
#

# General information
granule = datetime(2013, 3, 29, 0, 0)

# HIRS alg options
hirs_version = 'v20151014'
collo_version = 'v20151014'

platform_choices = ['noaa-06', 'noaa-07', 'noaa-08', 'noaa-09', 'noaa-10', 'noaa-11',
                    'noaa-12', 'noaa-14', 'noaa-15', 'noaa-16', 'noaa-17', 'noaa-18',
                    'noaa-19', 'metop-a', 'metop-b']

platform = 'metop-b'


def local_execute_example(granule, platform, hirs_version, collo_version, skip_prepare=False, skip_execute=False):

    LOG.info("We are doing {}".format(platform))
    comp_dict = {
        'granule': granule,
        'sat': platform,
        'hirs_version': hirs_version,
        'collo_version': collo_version
        }

    try:
        if not skip_prepare:
            LOG.info("Running local prepare...")
            local_prepare(comp, comp_dict)
        if not skip_execute:
            LOG.info("Running local execute...")
            local_execute(comp, comp_dict)
    except Exception, err:
        LOG.error("{}".format(err))
        LOG.info(traceback.format_exc())


def print_contexts(dt_left, dt_right, granule_length):
    interval = TimeInterval(dt_left, dt_right)
    contexts = comp.find_contexts(platform, hirs_version, collo_version, interval)
    contexts.sort()
    for context in contexts:
        print context

    return contexts

# local_execute_example(datetime(2016, 3, 18), 'metop-b', 'v20151014')