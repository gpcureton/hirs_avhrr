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
import shutil
import traceback
from glob import glob
from subprocess import CalledProcessError

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta
from flo.util import augmented_env
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    #check_call,
    #dawg_catalog,
    delivered_software,
    support_software,
    runscript,
    prepare_env,
    #nc_gen,
    nc_compress,
    hdf_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs2nc as hirs2nc
from flo.sw.hirs2nc.delta import DeltaCatalog

# every module should have a LOG object
LOG = logging.getLogger(__name__)

SPC = StoredProductCatalog()

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS_AVHRR(Computation):

    parameters = ['granule', 'satellite', 'hirs2nc_delivery_id', 'hirs_avhrr_delivery_id']
    outputs = ['out']

    def find_contexts(self, time_interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id):

        global delta_catalog

        LOG.debug('delta_catalog.collection = {}'.format(delta_catalog.collection))
        LOG.debug('delta_catalog.input_data = {}'.format(delta_catalog.input_data))

        # Using HIR1B file info as the baseline for the required contexts
        files = delta_catalog.files('hirs', satellite, 'HIR1B', time_interval)
        #files = delta_catalog.files('avhrr', satellite, 'PTMSX', time_interval)

        return [{'granule': file.data_interval.left,
                 'satellite': satellite,
                 'hirs2nc_delivery_id': hirs2nc_delivery_id,
                 'hirs_avhrr_delivery_id': hirs_avhrr_delivery_id}
                for file in files
                if file.data_interval.left >= time_interval.left]

    def hirs_to_time_interval(self, filename):
        '''
        Takes the HIRS filename as input and returns the 1-day time interval
        covering that file.
        '''

        file_chunks = filename.split('.')
        begin_time = datetime.strptime('.'.join(file_chunks[3:5]), 'D%y%j.S%H%M')
        end_time = datetime.strptime('.'.join([file_chunks[3], file_chunks[5]]), 'D%y%j.E%H%M')

        if end_time < begin_time:
            end_time += timedelta(days=1)

        return TimeInterval(begin_time, end_time)

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='NSS.GHRR')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        sensor = 'avhrr'
        satellite =  context['satellite']
        file_type = 'PTMSX'
        granule = context['granule']

        hirs_context = context.copy()
        hirs_context.pop('hirs_avhrr_delivery_id')
        LOG.debug("hirs_context: {}".format(hirs_context))

        # Initialize the hirs2nc module with the data locations
        hirs2nc.delta_catalog = delta_catalog
        # Instantiate the hirs2nc computation
        hirs2nc_comp = hirs2nc.HIRS2NC()

        LOG.debug("hirs input: {}".format(SPC.exists(hirs2nc_comp.dataset('out').product(hirs_context))))

        hirs_file = hirs2nc_comp.dataset('out').product(hirs_context)
        ptmsx_file = delta_catalog.file(sensor, satellite, file_type, granule)

        LOG.debug('data_interval = {}'.format(ptmsx_file.data_interval))

        task.input('HIR1B', hirs_file)
        task.input('PTMSX', ptmsx_file)
        task.option('data_interval', ptmsx_file.data_interval)

    def hirs_avhrr_collocation(self, inputs, context):
        '''
        Run the collocation executable.
        '''

        rc = 0

        LOG.info('inputs = {}'.format(inputs))

        satellite = context['satellite']
        hirs_avhrr_delivery_id = context['hirs_avhrr_delivery_id']

        hirs_file = inputs['HIR1B']
        patmosx_file = inputs['PTMSX']

        # Where are we running the package
        work_dir = abspath(curdir)
        LOG.debug("working dir = {}".format(work_dir))

        # Get the required collocation exe
        delivery = delivered_software.lookup('hirs_avhrr', delivery_id=hirs_avhrr_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        hirs_avhrr_bin = pjoin(dist_root, 'bin/hirs_avhrr_v4.exe')
        version = delivery.version

        # Get the required  environment variables
        env = prepare_env([delivery])
        LOG.debug(env)

        # Removing the output file if it exists
        granule_datestamp = '.'.join(basename(hirs_file).split('.')[3:8])
        output_file = 'colloc.hirs.avhrr.{}.{}.v{}.hdf'.format(satellite, granule_datestamp, version)
        if os.path.exists(output_file):
            LOG.info('{} exists, removing...'.format(output_file))
            os.remove(output_file)

        cmd = '{} {} {} {}'.format(hirs_avhrr_bin, hirs_file, patmosx_file, output_file)
        #cmd = 'sleep 1; touch {}'.format(output_file)

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_hirs_avhrr = 0
            runscript(cmd, requirements=[], env=env)
        except CalledProcessError as err:
            rc_hirs_avhrr = err.returncode
            LOG.error("hirs_avhrr binary {} returned a value of {}".format(hirs_avhrr_bin, rc_hirs_avhrr))
            return rc_hirs_avhrr, []

        # Verify output file
        output_file = glob(output_file)
        if len(output_file) != 0:
            output_file = output_file[0]
            LOG.info('Found collocation file "{}"'.format(output_file))
        else:
            LOG.error('There are no output collocation file "{}", aborting'.format(output_file))
            rc = 1
            return rc, []

        LOG.info('Finished collocation routine')

        return rc, output_file

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='NSS.GHRR')
    def run_task(self, inputs, context):

        LOG.debug("Running run_task()...")

        for key in context.keys():
            LOG.debug("run_task() context['{}'] = {}".format(key, context[key]))

        granule = context['granule']

        rc, colloc_file = self.hirs_avhrr_collocation(inputs, context)

        LOG.debug('hirs_avhrr_collocation() return value: {}'.format(rc))
        LOG.info('hirs_avhrr_collocation() generated {}'.format(colloc_file))

        # The staging routine assumes that the output file is located in the work directory
        # "tmp******", and that the output path is to be prepended, so return the basename.
        output = basename(colloc_file)

        data_interval = context['data_interval']
        extra_attrs = {'begin_time': data_interval.left,
                       'end_time': data_interval.right}

        #return {'out': hdf_compress(output)}
        return {'out': {'file': hdf_compress(output), 'extra_attrs': extra_attrs}}
