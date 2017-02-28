import tarfile

from jsub.error import BackendNotFoundError

from jsub.mixin.backend.common import Common

class Dirac(Common):
    def __init__(self, param):
        self.__initialize_dirac()

        self._param = param

        self._logger = logging.getLogger('JSUB')

        self.initialize_common_param()
        self._foreground = param.get('foreground', False)
        self._max_submit = param.get('max_submit', 4)

    def __initialize_dirac(self):
        try:
            from DIRAC.Core.Base import Script
            Script.initialize(ignoreErrors=True)
            from DIRAC.Interfaces.API.Dirac import Dirac as DiracClient
            from DIRAC.Interfaces.API.Job import Job as DiracJob
        except ImportError as e:
            raise BackendNotFoundError('DIRAC client not properly setup: %s' % e)

    def property(self):
        return {'run_on': 'remote'}

    def __pack_main_root(self, main_root_dir, pack_path):
        with tarfile.open(pack_path, 'w:gz') as tar:
            tar.add(main_root_dir)

    def submit(self, task_id, sub_ids, launcher_exe):
        work_root = self.get_work_root(task_id)

        main_root_dir = os.path.join(work_root, 'main')
        main_pack_file = os.path.join(work_root, 'jsub_main_root.tar.gz')
        self.__pack_work_dir(main_root_dir, main_pack_file)

        return {}
#        input_sandbox()
#        output_sandbox()
#        site()

        processes = {}

        count = 0
        for sub_id in sub_ids:
            if count >= self._max_submit:
                break

            try:
                launcher = os.path.join(self.work_root(task_id), launcher_exe)
                FNULL = open(os.devnull, 'w')
                process = subprocess.Popen([launcher, str(sub_id)], stdout=FNULL, stderr=subprocess.STDOUT)
                start_time = _process_start_time(process.pid)
            except OSError as e:
                self._logger.error('Submit job (%s.%s) to "local" failed: %s' % (task_id, sub_id, e))
                continue

            count += 1
            processes[sub_id] = {}
            processes[sub_id]['process'] = process
            processes[sub_id]['start_time'] = start_time

        if self._foreground:
            for _, data in processes.items():
                data['process'].wait()

        result = {}
        for sub_id, data in processes.items():
            result[sub_id] = '%s_%s' % (data['start_time'], data['process'].pid)
        return result
