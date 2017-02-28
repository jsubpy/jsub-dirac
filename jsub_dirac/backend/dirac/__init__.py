import os
import logging
import tarfile

from jsub.error import BackendNotFoundError

from jsub.mixin.backend.common import Common


try:
    from DIRAC.Core.Base import Script
    Script.initialize(ignoreErrors=True)
    from DIRAC.Interfaces.API.Dirac import Dirac as DiracClient
    from DIRAC.Interfaces.API.Job import Job as DiracJob
except ImportError as e:
    raise BackendNotFoundError('DIRAC client not properly setup: %s' % e)


class Dirac(Common):
    def __init__(self, param):
        self._param = param

        self._logger = logging.getLogger('JSUB')

        self.__site = param.get('site', [])
        self.__banned_site = param.get('site', [])

        self.initialize_common_param()

    def property(self):
        return {'run_on': 'remote'}

    def __pack_main_root(self, main_root_dir, pack_path):
        with tarfile.open(pack_path, 'w:gz') as tar:
            tar.add(main_root_dir, arcname='main')

    def submit(self, task_id, sub_ids, launcher_param):
        work_root = self.get_work_root(task_id)

        main_root_dir = os.path.join(work_root, 'main')
        main_pack_file = os.path.join(work_root, launcher_param['main_pack_file'])
        self.__pack_main_root(main_root_dir, main_pack_file)

        launcher_exe = launcher_param['executable']
        launcher_path = os.path.join(work_root, launcher_exe)

        str_sub_ids = [str(sub_id) for sub_id in sub_ids]

        j = DiracJob()
        j.setName('jsub')
        j.setExecutable(launcher_exe)
        j.setParameterSequence('JobName', str_sub_ids, addToWorkflow=True)
        j.setParameterSequence('arguments', str_sub_ids, addToWorkflow=True)
        j.setInputSandbox([launcher_path, main_pack_file])
        j.setOutputSandbox(['jsub_log.tar.gz'])

        if self.__site:
            j.setDestination(self.__site)
        if self.__banned_site:
            j.setBannedSites(self.__banned_site)

        dirac = DiracClient()
        result = dirac.submit(j)

        final_result = {}
        for sub_id, dirac_id in zip(sub_ids, result['Value']):
            final_result[sub_id] = dirac_id
        return final_result
