#!/usr/bin/env python

import os
import sys
import json

try:
	from DIRAC.Core.Base import Script
	from IHEPDIRAC.WorkloadManagementSystem.Client.TaskClient import TaskClient

except ImportError as e:
	sys.stdout.write('IHEP-DIRAC client not properly setup: %s\n' % e)
	sys.exit(1)

Script.setUsageMessage('Run DIRAC sub command')
# submit
Script.registerSwitch('', 'name=', 'Job name')
Script.registerSwitch('', 'job-group=', 'Job group')
Script.registerSwitch('', 'task-id=', 'Task id')
Script.registerSwitch('', 'sub-ids=', 'Job sub id list')
Script.registerSwitch('', 'input-sandbox=', 'Input sandbox')
Script.registerSwitch('', 'output-sandbox=', 'Output sandbox')
Script.registerSwitch('', 'executable=', 'Job executable')
Script.registerSwitch('', 'site=', 'Job sites')
Script.registerSwitch('', 'banned-site=', 'Job banned sites')
# delete, reschedule
Script.registerSwitch('', 'backend-task-id=', 'Backend task id')
Script.registerSwitch('', 'job-status=', 'List of job status to match')
Script.registerSwitch('', 'backend-ids=', 'Dirac job id list')
Script.parseCommandLine(ignoreErrors=False)


from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.Job import Job
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import gConfig,gLogger,S_ERROR,S_OK

def status(backend_task_id, job_status):
	taskClient = TaskClient()
	jobMonitor= RPCClient('WorkloadManagement/JobMonitoring')

	if not job_status:
		result=taskClient.getTaskProgress(backend_task_id)
		if 'Value' in result:
			result['njobs'] = result['Value']
		return result
	else:
		jobs=taskClient.getTaskJobs(backend_task_id)['Value']
		job_list={status:[] for status in job_status}
		for job in jobs:
			try: 
				status=jobMonitor.getJobStatus(int(job))['Value']
				if status in job_status:
					job_list[status].append(int(job))
			except:
				pass
		return {'OK':True,'jobIDs':job_list}

def delete(backend_task_id,job_status):
	taskClient = TaskClient()
	result = taskClient.deleteTask(backend_task_id, job_status)

	return result

def reschedule(backend_task_id, job_status, sub_ids, backend_ids):
	taskClient = TaskClient()
	jobManager= RPCClient('WorkloadManagement/JobManager')
	if job_status:
		result = taskClient.rescheduleTask(backend_task_id, job_status)
		return result
	elif sub_ids:
		#jsub would match sub_ids with backend_ids to avoid executing codes here.
		pass
	elif backend_ids:
		result = jobManager.rescheduleJob(backend_ids)
		return result
	
	return{'OK':True,'Value': []}
	

def submit(name, job_group, task_id,  input_sandbox, output_sandbox, executable, site=None, banned_site=None, sub_ids=[]):
	dirac = Dirac()
	
	submit_result = {'backend_job_ids':{}}
	jobInfos={}

	for run in range(int((len(sub_ids)+99)/100)):
		ids_this_run = [x for x in sub_ids[run*100:(run+1)*100]]
		job_names = ['%s.%s' % (name, sub_id) for sub_id in ids_this_run]
		j = Job()
		j.setName(name)
		j.setExecutable(executable)

		j.setParameterSequence('JobName', job_names, addToWorkflow=True)
		j.setParameterSequence('arguments', ids_this_run, addToWorkflow=True)

		if input_sandbox:
			j.setInputSandbox(input_sandbox)
		if output_sandbox:
			j.setOutputSandbox(output_sandbox)

		if job_group:
			j.setJobGroup(job_group)
		if site:
			j.setDestination(site)
		if banned_site:
			j.setBannedSites(banned_site)

		result = dirac.submitJob(j)


		if not result['OK']:
			sys.stdout.write('DIRAC job submit error: %s\n' % result['Message'])
			sys.exit(1)

		for sub_id, dirac_id in zip(ids_this_run, result['Value']):
			submit_result['backend_job_ids'][sub_id] = dirac_id
			jobInfos[dirac_id]={'SubID':sub_id}

	#Register on Task-manager Webapp of IHEPDIRAC
	task = RPCClient('WorkloadManagement/TaskManager')
	taskInfo={'TaskName':name, 'JobGroup': job_group, 'JSUB-ID': task_id}
	task_result=task.createTask(name, taskInfo, jobInfos)
	task_web_id=task_result['Value']
	submit_result['backend_task_id']=task_web_id

	return submit_result


def getListArg(arg):
	if not arg:
		return []
	return arg.split(',')

def main():
	args = Script.getPositionalArgs()
	switches = Script.getUnprocessedSwitches()

	name = 'jsub'
	job_group = 'jsub-job'
	task_id = 0
	sub_ids = []
	backend_ids = []
	input_sandbox = []
	output_sandbox = []
	executable = ''
	site = None
	banned_site = None

	backend_task_id = 0
	job_status = []

	for k, v in switches:
		if k == 'name':
			name = v
		elif k == 'job-group':
			job_group = v
		elif k == 'task-id':
			task_id = int(v)
		elif k == 'sub-ids':
			sub_ids = getListArg(v)
		elif k == 'backend-ids':
			backend_ids = getListArg(v)
		elif k == 'input-sandbox':
			input_sandbox = getListArg(v)
		elif k == 'output-sandbox':
			output_sandbox = getListArg(v)
		elif k == 'executable':
			executable = v
		elif k == 'site':
			site = getListArg(v)
		elif k == 'banned-site':
			banned_site = getListArg(v)
		elif k == 'backend-task-id':
			backend_task_id = v
		elif k == 'job-status':
			job_status = getListArg(v)

	if args[0] == 'submit':
		result = submit(name, job_group, task_id, 
						input_sandbox, output_sandbox, executable, site, banned_site, sub_ids=sub_ids)
	elif args[0] == 'delete':
		result = delete(int(backend_task_id), job_status)
	elif args[0] == 'reschedule':
		result = reschedule(int(backend_task_id), job_status, sub_ids, backend_ids)
	elif args[0] == 'status':
		result = status(int(backend_task_id), job_status)

	print(json.dumps(result))

	return 0


if __name__ == '__main__':
	exit(main())
