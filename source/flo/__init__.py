#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_avhrr package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import sys
import logging
import traceback

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta
from flo.util import augmented_env, symlink_inputs_to_working_dir
#from flo.subprocess import check_call
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    #dawg_catalog,
    #delivered_software,
    #support_software,
    #runscript,
    #prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs as hirs
from flo.sw.hirs.delta import DeltaCatalog

# every module should have a LOG object
LOG = logging.getLogger(__name__)

SPC = StoredProductCatalog()

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS_AVHRR(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version']
    outputs = ['out']

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='NSS.GHRR')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        sensor = 'avhrr'
        sat =  context['sat']
        file_type = 'PTMSX'
        granule = context['granule']

        hirs_context = context.copy()
        hirs_context.pop('collo_version')
        LOG.debug("hirs_context: {}".format(hirs_context))

        # Initialize the hirs module with the data locations
        hirs.delta_catalog = delta_catalog
        # Instantiate the hirs computation
        hirs_comp =  hirs.HIRS()

        LOG.debug("hirs input: {}".format(SPC.exists(hirs_comp.dataset('out').product(hirs_context))))

        LOG.debug('Getting HIR1B input...')
        task.input('HIR1B', hirs_comp.dataset('out').product(hirs_context))
        LOG.debug('Getting PTMSX input...')
        task.input('PTMSX', delta_catalog.file(sensor, sat, file_type, granule))

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='NSS.GHRR')
    def run_task(self, inputs, context):

        LOG.debug("Running run_task()...")

        inputs = symlink_inputs_to_working_dir(inputs)
        output = 'hirs.avhrr.{}.{}.collo'.format(context['sat'], inputs['HIR1B'][12:30])

        lib_dir = pjoin(self.package_root, context['hirs_version'], 'lib')

        cmd = pjoin(self.package_root, context['hirs_version'], 'c++/hirs_avhrr')
        cmd += ' {} {} {}'.format(inputs['HIR1B'], inputs['PTMSX'], output)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'out': output}

    def find_contexts(self, time_interval, sat, hirs_version, collo_version):

        global delta_catalog

        LOG.debug('delta_catalog.collection = {}'.format(delta_catalog.collection))
        LOG.debug('delta_catalog.input_data = {}'.format(delta_catalog.input_data))

        # Using HIR1B file info as the baseline for the required contexts
        files = delta_catalog.files('hirs', sat, 'HIR1B', time_interval)
        #files = delta_catalog.files('avhrr', sat, 'PTMSX', time_interval)

        return [{'granule': file.data_interval.left,
                 'sat': sat,
                 'hirs_version': hirs_version,
                 'collo_version': collo_version}
                for file in files
                if file.data_interval.left >= time_interval.left]
