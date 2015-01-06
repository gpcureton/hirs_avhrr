
import os
import shutil
from flo_deploy.packagelib import *

class HIRS_AVHRR_Package(Package):

    def deploy_package(self):
        for ver in ['v20140204']:
            self.merge(CppCollocation().path(), 'v20140204')
            self.merge(Netcdf().path(), 'v20140204')
            self.merge(Hdf5().path(), 'v20140204')

class CppCollocation(Resource):

    git_url = 'ssh://versionitis.ssec.wisc.edu/home/gregq/git/collocation.git'
    git_ref = 'master'

    def cache_subpath(self):
        return 'build/collocation'

    def deploy(self, dst_dir):
        shutil.copytree(GitCheckout(self.git_url, self.git_ref).path(), dst_dir)
        self.build_cpp(dst_dir)

    def build_cpp(self, build_dir):
        cpp_dir = os.path.join(build_dir, 'c++')
        check_call(['autoconf'], cwd=cpp_dir)
        check_call(['./configure',
                    '--with-hdf=' + Hdf4().path(), '--with-hdf5=' + Hdf5(static=True).path(),
                    '--with-netcdf=' + Netcdf().path()],
                   cwd=cpp_dir)
        check_call(['make'], cwd=cpp_dir)
