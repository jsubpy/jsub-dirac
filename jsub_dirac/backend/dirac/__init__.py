import os
import subprocess
import json
import logging
import tarfile
import glob

from jsub.error import BackendNotFoundError

from jsub.mixin.backend.common import Common

from jsub.util import ensure_list

DIRAC_BACKEND_DIR = os.path.dirname(os.path.realpath(__file__))


class Dirac(Common):
	def __init__(self, param):
		self._param = param
		self._logger = logging.getLogger('JSUB')

		self.__job_group = param.get('jobGroup')
		self.__job_name = param.get('jobName')
		self.__banned_site = ensure_list(param.get('bannedSite', []))
		if not self.__banned_site:
			self.__banned_site = ensure_list(param.get('bannedSites', []))
		self.__site = ensure_list(param.get('site', []))
		if not self.__site:
			self.__site = ensure_list(param.get('sites', []))

		self.initialize_common_param()

	def property(self):
		return {'run_on': 'remote','name':'dirac'}

	def __pack_main_root(self, main_root_dir, pack_path):
		with tarfile.open(pack_path, 'w:gz') as tar:
			tar.add(main_root_dir, arcname='main')
	
	def get_log(self, task_data = None, path = './', sub_ids = [], status = [], njobs = 10):
		getlog_result={}
		parent_dir = os.path.realpath(path if path else '.')

		# generate sid_list from sub_ids and status filter
		sid_list=[]
		sid_list.extend(sub_ids)
		if len(status)>0:
			status_text=''.join([(s[0] if s!='Deleted' else 'K') for s in status])
			status_result=self.status(task_data.get('backend_task_id'),status_text)
			if status_result['OK']:
				for s in status:
					for jid,bid in task_data.get('backend_job_ids').items():
						if bid in status_result['jobIDs'][s]:
							sid_list.append(jid)

		sid_list=list(set(sid_list))	#get rid of repetitive sub ids
		if len(sid_list)> int(njobs):
			sid_list=sid_list[:int(njobs)]
			self._logger.info('Exceeding max njobs, only output the log files of first %s jobs.'%njobs)
#		self._logger.info('ID of relevant subjobs: %s'%str(sid_list))



		for sid in sid_list:
			is_ok=True
			message=''
			backend_job_id = task_data.get('backend_job_ids',{}).get(str(sid))
			if not backend_job_id:
				is_ok = False
				message = 'Cannot find correspondent job on Dirac backend'			
			if is_ok:
				subjob_dir = os.path.join(parent_dir,str(sid))
				if os.path.isdir(os.path.abspath(subjob_dir)):
					os.system('rm -r %s'%os.path.abspath(subjob_dir))
				res = os.system('mkdir -p %s'%subjob_dir)
				
				os.chdir(subjob_dir)
				if res!=0:
					is_ok = False
					message = 'Cannot create directory for the log files.'
				if is_ok:
					try:
						cmd = [os.path.join(DIRAC_BACKEND_DIR, 'script', 'getlog.sh')]
						cmd += [str(backend_job_id)]
						output = subprocess.check_output(cmd)
						os.chdir(str(backend_job_id))
#						os.system('pwd')
#						print('Unpacking log files of subjob %s:'%sid)
						os.system('tar -xvf jsub_log.tar.gz  > /dev/null')
#						print('')
						os.chdir('../')
						os.system('find ./*/ |grep launcher > tmp_logfile_dir;dirname `cat tmp_logfile_dir` >tmp_logfile_dir;mv `cat tmp_logfile_dir`/* . >> /dev/null;rm bootstrap* tmp_logfile*')
#						os.system('mv `find ./*/ |grep navigator` ./')
						os.system('rm -rf %s'%backend_job_id)
					except subprocess.CalledProcessError as e:
						self._logger.error('Failed to retrieve log files of subjob %s, with the following message:'%str(sid))
						error_message=e.stdout.decode('UTF-8')
						if len(error_message)<2:
							error_message=None
						print(error_message)
						is_ok = False
						message = 'Failed to download log files from Dirac backend.'	
			getlog_result.update({sid:{'OK':is_ok,'Message':message}})
		return getlog_result


	def status(self, backend_task_id, job_status, silent=False):
		cmd = [os.path.join(DIRAC_BACKEND_DIR, 'script', 'dirac-run.sh')]
		cmd += ['status']
		cmd += ['--backend-task-id',str(backend_task_id)]
		
		if job_status:
			status =[]
			if (('D' in job_status) and ('Delete' not in job_status)) or ('done' in job_status):
				status.append('Done')
			if [x for x in ['wait','W','checking','Checking'] if x in job_status]:
				status.append('Waiting')
				status.append('Checking')
			if [x for x in ['fail','F'] if x in job_status]:
				status.append('Failed')
			if [x for x in ['run','R'] if x in job_status]:
				status.append('Running')
			if [x for x in ['deleted','Delete','K','killed'] if x in job_status]:
				status.append('Deleted')
				status.append('Killed')
			str_status = ','.join(status)
			if str_status:	
				cmd += ['--job-status', str_status]
		try:
			output_json = subprocess.check_output(cmd)
			output=json.loads(output_json)
		except subprocess.CalledProcessError as e:
			if not silent:
				self._logger.error('Failed to retrieve subjob statuses, script returncode=%s with the following stdout:' % e.returncode)
				message=e.stdout.decode('UTF-8')
				if len(message)<=1:
					message='None'
				print(message)
			output = {'OK':False, 'Message': 'Failed to retrieve job status %s' % e}

		return output
		
	def delete_task(self, backend_task_id, job_status):
		cmd = [os.path.join(DIRAC_BACKEND_DIR, 'script', 'dirac-run.sh')]
		cmd += ['delete']
		job_status=ensure_list(job_status)
		cmd +=['--backend-task-id',str(backend_task_id)]
		cmd +=['--job-status',','.join(job_status)]


		try:
			output = subprocess.check_output(cmd)
		except subprocess.CalledProcessError as e:
			self._logger.error('Failed to delete task on Dirac backend: %s' % e)
			return 

		return json.loads(output)

	def reschedule(self, backend_task_id,status=None,sub_ids=None,backend_ids=None):
		cmd = [os.path.join(DIRAC_BACKEND_DIR, 'script', 'dirac-run.sh')]
		cmd += ['reschedule']
		cmd += ['--backend-task-id',str(backend_task_id)]
		if status:
			status = ensure_list(status)
			cmd+=['--job-status',','.join(status)]
		elif sub_ids:	
			sub_ids = ensure_list(sub_ids)
			cmd+=['--sub-ids',','.join([str(x) for x in sub_ids])]
		elif backend_ids:
			backend_ids = ensure_list(backend_ids)
			cmd+=['--backend-ids',','.join([str(x) for x in backend_ids])]

		try:
			output = subprocess.check_output(cmd)
		except subprocess.CalledProcessError as e:
			self._logger.error('Failed to reschedule task on Dirac backend: %s' % e)
			return

		return json.loads(output)


	def submit(self, task_id,  launcher_param, sub_ids=None):
		run_root = self.get_run_root(task_id)

		username=os.environ.get('USER','')

		if self.__job_group is None:
			self.__job_group = 'jsub.%s.%s' %(username,task_id)
		else:
			self.__job_group = self.__job_group
		if self.__job_name is None:
			self.__job_name = 'jsub.%s.%s' % (username,task_id)

		main_root_dir = os.path.join(run_root, 'main')
		main_pack_file = os.path.join(run_root, launcher_param['main_pack_file'])
		self.__pack_main_root(main_root_dir, main_pack_file)

		launcher_exe = launcher_param['executable']
		launcher_path = os.path.join(run_root, launcher_exe)


		cmd = [os.path.join(DIRAC_BACKEND_DIR, 'script', 'dirac-run.sh')]
		cmd += ['submit']
		cmd += ['--name', self.__job_name]
		cmd += ['--job-group', self.__job_group]
		cmd += ['--task-id', str(task_id)]
		if sub_ids:
			str_sub_ids = [str(sub_id) for sub_id in sub_ids]
			cmd += ['--sub-ids', ','.join(str_sub_ids)]
		cmd += ['--input-sandbox', '%s,%s' % (launcher_path, main_pack_file)]
		cmd += ['--output-sandbox', '{jsub_log.tar.gz, jsub.out}']
		cmd += ['--executable', launcher_exe]
		cmd += ['--site', ','.join(self.__site)]
		cmd += ['--banned-site', ','.join(self.__banned_site)]

		#use command to call subprocess to avoid conflict of envs (for dirac and for jsub)
		try:
			output = subprocess.check_output(cmd)
		except subprocess.CalledProcessError as e:
			output=e.stdout
			returncode=e.returncode
			self._logger.error('Submit job to DIRAC failed: submission script exited with return code %s'%str(returncode))
			#give suggestion based on return code
			if returncode==1:
				self._logger.error('DIRAC environment not properly setup?')
			elif returncode==2:
				self._logger.error('dirac.submitJob() failed.')
			

			#printing error message
			try:
				message=output.decode('UTF-8')
				if len(message)<=1:
					message='None'
				self._logger.error('stdout of submission script:')
				print(message)
			except:
				self._logger.error('Failed to retrieve stdout of submission script.')
			return

		return json.loads(output)
