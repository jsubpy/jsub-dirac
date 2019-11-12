import os
import subprocess
import json
import logging
import tarfile

from jsub.error import BackendNotFoundError

from jsub.mixin.backend.common import Common

from jsub.util import ensure_list

DIRAC_BACKEND_DIR = os.path.dirname(os.path.realpath(__file__))


class Dirac(Common):
    def __init__(self, param):
        self._param = param

        self._logger = logging.getLogger('JSUB')

        self.__site = ensure_list(param.get('site', []))
        self.__banned_site = ensure_list(param.get('banned_site', []))

        self.initialize_common_param()

    def property(self):
        return {'run_on': 'remote'}

    def __pack_main_root(self, main_root_dir, pack_path):
        with tarfile.open(pack_path, 'w:gz') as tar:
            tar.add(main_root_dir, arcname='main')

    def submit(self, task_id, sub_ids, launcher_param):
        work_root = self.get_work_root(task_id)

        main_root_dir = os.path.join(work_root, 'main')
        main_pack_file = os.path.join(
            work_root, launcher_param['main_pack_file'])
        self.__pack_main_root(main_root_dir, main_pack_file)

        launcher_exe = launcher_param['executable']
        launcher_path = os.path.join(work_root, launcher_exe)

        str_sub_ids = [str(sub_id) for sub_id in sub_ids]

        cmd = [os.path.join(DIRAC_BACKEND_DIR, 'script', 'dirac-run.sh')]
        cmd += ['submit']
        cmd += ['--name', 'jsub']
        cmd += ['--job-group', 'jsub-%s' % task_id]
        cmd += ['--task-id', str(task_id)]
        cmd += ['--sub-ids', ','.join(str_sub_ids)]
        cmd += ['--input-sandbox', '%s,%s' % (launcher_path, main_pack_file)]
        cmd += ['--output-sandbox', 'jsub_log.tar.gz']
        cmd += ['--executable', launcher_exe]
        cmd += ['--site', ','.join(self.__site)]
        cmd += ['--banned-site', ','.join(self.__banned_site)]

        try:
            output = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            self._logger.error('Submit job to DIRAC failed: %s' % e)
            return

        return json.loads(output)
