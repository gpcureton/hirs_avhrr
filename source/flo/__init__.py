
import os
from flo.computation import Computation
from flo.subprocess import check_call
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.sw.hirs import HIRS
from flo.sw.hirs.delta import delta_catalog


class HIRS_AVHRR(Computation):

    parameters = ['granule', 'sat', 'hirs_version', 'collo_version']
    outputs = ['out']

    def build_task(self, context, task):

        hirs_context = context.copy()
        hirs_context.pop('collo_version')

        task.input('HIR1B', HIRS().dataset('out').product(hirs_context))
        task.input('PTMSX', delta_catalog.file('avhrr', context['sat'], 'PTMSX',
                                               context['granule']))

    def run_task(self, inputs, context):

        inputs = symlink_inputs_to_working_dir(inputs)
        output = 'hirs.avhrr.{}.{}.collo'.format(context['sat'], inputs['HIR1B'][12:30])

        lib_dir = os.path.join(self.package_root, context['hirs_version'], 'lib')

        cmd = os.path.join(self.package_root, context['hirs_version'], 'c++/hirs_avhrr')
        cmd += ' {} {} {}'.format(inputs['HIR1B'], inputs['PTMSX'], output)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'out': output}

    def find_contexts(self, sat, hirs_version, collo_version, time_interval):

        files = delta_catalog.files('hirs', sat, 'HIR1B', time_interval)
        return [{'granule': file.data_interval.left,
                 'sat': sat,
                 'hirs_version': hirs_version,
                 'collo_version': collo_version}
                for file in files]
