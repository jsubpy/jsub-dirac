#!/usr/bin/env python

'''
This action module downloads files to the worknode.
	-accept LFNs of files in DFC or local files registered and uploaded to DFC
	-move file to destination (specified by folder or filename).
'''

import itertools
import sys
import os
import subprocess

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOForGroup

import DIRAC
from DIRAC.Interfaces.API.Dirac import Dirac

from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport

def input_path_to_lfn(input_path):

	res = getProxyInfo( False, False )
	if not res['OK']:
		gLogger.error( "Failed to get client proxy information.", res['Message'] )
		DIRAC.exit( 2 ) 
	proxyInfo = res['Value']
	if proxyInfo['secondsLeft'] == 0:
		gLogger.error( "Proxy expired" )
		DIRAC.exit( 2 )
	username = proxyInfo['username']
	vo = ''
	if 'group' in proxyInfo:
		vo = getVOForGroup( proxyInfo['group'] )

	folder_name=os.path.basename(os.path.dirname(input_path))
	lfn = '/%s/user/%s/%s/jsub/'%(vo,username[0],username) + folder_name + '/' + os.path.basename(input_path)
	gLogger.debug('input path = %s'% input_path)
	gLogger.debug('lfn translation = %s'% lfn)

	return(lfn)


def main():
	input_file_jobvar_name='JSUB_'+os.environ.get('JSUB_input_file_jobvar_name','input_file') # if input file is local, and put to DFC through jsub register.
	input_lfn_jobvar_name='JSUB_'+os.environ.get('JSUB_input_lfn_jobvar_name','input_lfn') # if input file is directly from DFC
	source_lfn_prefix=os.environ.get('JSUB_source_lfn_prefix')
	input_path=os.environ.get(input_file_jobvar_name)
	input_lfn=os.environ.get(input_lfn_jobvar_name)
	destination=os.environ.get('JSUB_destination','./')
	use_xcache=os.environ.get('JSUB_XCache',False)
	test_pfn=os.environ.get('JSUB_test_pfn','')

	if input_lfn is None:
		if source_lfn_prefix:
			lfn = os.path.join(source_lfn_prefix,input_path)
		else:
			lfn = input_path_to_lfn(input_path)
	else:
		lfn=input_lfn
	fname=os.path.basename(lfn)

	# send job message before downloading start
#	jobID = os.environ.get('DIRACJOBID', '0')
#	if jobID!='0':
#		jobReport = JobReport(int(jobID), 'JSUB script')
#		res_report = jobReport.setApplicationStatus("Start Downloading input data")
#		if not res_report['OK']:
#			gLogger.error('Failed to set dirac logging: %s' % res_report['Message'])
	

	# download the file to the action folder
	file_downloaded=False
	sitename = DIRAC.siteName()
	if str(use_xcache).upper()=='True': #try to download file with xcache
		for XCache in gConfig.getValue( 'Resources/XCaches/%s' % ( sitename ), [] ):
			xrdcp_url='xroot://%s/%s'%(XCache,test_pfn) 
			fname=os.path.basename(test_pfn)
			cmd='xrdcp %s %s'%(xrdcp_url,fname)
	else:		# simple xrdcp
		sitename = DIRAC.siteName()
		cmd='xrdcp %s %s'%(lfn,fname)
		
	print("executing cmd: %s"%cmd)
#	proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE, timeout=3600)  # timeout invalid in python 2
	proc = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
	return_code = proc.wait()
	#proc.stdout, proc.stderr
	if return_code==0:
		file_downloaded=True
		print("Successfully downloaded file: %s"%fname)

	if not file_downloaded: #use dirac API to download file
		dirac = Dirac()
		result=dirac.getFile(lfn)
	
	if not result['OK']:
		gLogger.error('Download file error: %s' % result['Message'])
		return 2

		
	# send message to DIRAC after downloading finish
#	if jobID!='0':
#		jobReport = JobReport(int(jobID), 'JSUB script')
#		res_report = jobReport.setApplicationStatus("Finished Downloading input data")
#		res_repor
#		if not res_report['OK']:
#			gLogger.error('Failed to set dirac logging: %s' % res_report['Message'])



	# mv to destination
	if not (os.path.samefile(fname,destination) or os.path.exists(os.path.join('./',os.path.basename(fname))) ):
		os.system('mv %s %s'%(fname, destination))

	return 0


if __name__ == '__main__':
	exit(main())



