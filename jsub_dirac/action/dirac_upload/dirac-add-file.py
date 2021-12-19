#!/usr/bin/env python

'''
This action module uploads files on WN to DFC.
'''

import os
import glob

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

from DIRAC.WorkloadManagementSystem.Client.JobReport import JobReport


def getUserHome():
	res = getProxyInfo(False, False)
	if not res['OK']:
		return ''
	owner = res['Value']['username']
	ownerGroup = res['Value']['group']
	vo = Registry.getVOMSVOForGroup(ownerGroup)
	voHome = '/{0}'.format(vo)
	userHome = '/{0}/user/{1:.1}/{1}'.format(vo, owner)
	return userHome


def main():
	source_location=os.environ.get('JSUB_source_location','./')
	destination_dir=os.environ.get('JSUB_destination_dir','')
	failable_file=eval(os.environ.get('JSUB_failable_file','[]'))		# files in this list shouldn't generate error when they can't be uploaded
	destination_dir_jobvar=os.environ.get('JSUB_dirac_upload_destination_dir_jobvar')	# allow using $(jobvar) when defining path
	if destination_dir_jobvar is not None:	
		destination_dir = destination_dir_jobvar
	destination_in_user_home=(os.environ.get('JSUB_user_home',"True").upper() == "TRUE")	#if true: relative path to cefs user home for upload destination; if false: absolute path
	files_to_upload=os.environ.get('JSUB_files_to_upload','*')
	upload_dict=eval(os.environ.get('JSUB_upload_dict','{}')) 	## {file_to_upload: folder}
	overwrite=(os.environ.get('JSUB_overwrite','False').upper() == 'TRUE')
	upload_file_jobvar=os.environ.get('JSUB_upload_file_jobvar')
	relocate_to_cwd=os.environ.get('JSUB_relocate_to_cwd')		#if true, value of jobvar doesn't reflect dir name; file is under cwd instead
	se = os.environ.get('JSUB_SE','IHEP-STORM')
	if se is None or str(se).upper()=='NONE':
		se='IHEP-STORM'
	upload_status=0

	ff=[]
	for x in failable_file:
		ff_raw=glob.glob(os.path.join(source_location,x))
		ff_raw=[os.path.relpath(x,source_location) for x in ff_raw]
		ff+=ff_raw
	ff=list(set(ff))
	

	if not upload_dict:
		# if upload_file_jobvar exists, need to reshape output setting to a standard one
		if upload_file_jobvar is not None:
			if relocate_to_cwd:		#value of jobvar doesn't reflect dir name; file is under cwd instead
				upfile=os.environ.get('JSUB_'+upload_file_jobvar)
				source_location='./'
				files_to_upload=os.path.basename(upfile)
				if destination_in_user_home:
					destination_dir=os.path.dirname(upfile)
			else:
				files_to_upload=os.environ.get('JSUB_'+upload_file_jobvar)
	#			destination_dir=os.path.dirname(upfile)
	
		# files_to_upload should be wildcards splitted by ','
		flist_raw=files_to_upload.split(',')
		flist=[]
		for f in flist_raw:
			l=glob.glob(os.path.join(source_location,f))
			l=[os.path.relpath(x,source_location) for x in l]
			flist+=l
		flist=list(set(flist))	# remove repeated items
	
		userHome = getUserHome()
		if not userHome:
			gLogger.error('Failed to get user home')
			return 1
	
		# send message to DIRAC logging before start
	#	jobID = os.environ.get('DIRACJOBID', '0')
	#	if jobID!='0':
	#		jobReport = JobReport(int(jobID), 'JSUB script')
	#		res_report = jobReport.setApplicationStatus("Start Uploading")
	#		if not res_report['OK']:
	#			gLogger.error('Failed to set dirac logging: %s' % res_report['Message'])

		print("Files to upload: {0}".format(str(flist)))	

		# uploading files
		for f in flist:
			if destination_in_user_home:
				userHome = getUserHome()
				if not userHome:
					gLogger.error('Failed to get user home')
					return 1
				lfn = os.path.join(userHome, destination_dir, f)
			else:
				lfn = os.path.join(destination_dir,f)
		
			fcc = FileCatalogClient()
			dm = DataManager(['FileCatalog'])
		
			print('registering file: LFN={0}, source={1}, SE={2}'.format(lfn,os.path.join(source_location,f),se))
			res = dm.putAndRegister(lfn, os.path.join(source_location,f), se, overwrite=overwrite)
			if not res['OK']:
				gLogger.error('Failed to putAndRegister %s \nto %s \nwith message: %s' % (
					lfn, se, res['Message']))
				upload_status=1
			elif res['Value']['Failed'].has_key(lfn):
				gLogger.error('Failed to putAndRegister %s to %s' % (lfn, se))
				upload_status=1
	
		# send message to DIRAC logging after finish
	#	if jobID!='0':
	#		jobReport = JobReport(int(jobID), 'JSUB script')
	#		res_report = jobReport.setApplicationStatus("Finished Uploading")
	#		if not res_report['OK']:
	#			gLogger.error('Failed to set dirac logging: %s' % res_report['Message'])


	else: # upload dict is present
		fgroups=upload_dict.keys()
		uploaded_files=set()		# keep track to avoid repeated uploading
		print('Uploading files to paths: {0} '.format(str(upload_dict)))
		print('File list in current working dir:')
		os.system('ls;echo "";')
		for fgroup in fgroups:
			
			folder=upload_dict[fgroup]
			if ('COMPSTR' in folder.upper()) or ('COMPOSITE' in folder.upper()):	#{jobvar_name: 'COMPSTR'}
				jobvar_name=fgroup		 
				lfn=os.environ.get('JSUB_'+jobvar_name)	# get resolved jobvar value
				f=os.path.basename(lfn)					

				fcc = FileCatalogClient()
				dm = DataManager(['FileCatalog'])
					
	
				print('registering file: LFN={0}, source={1}, SE={2}'.format(lfn,os.path.join(source_location,f),se))
				if f not in uploaded_files:
					res = dm.putAndRegister(lfn, os.path.join(source_location,f), se, overwrite=overwrite)
				if not res['OK']:
					gLogger.error('Failed to putAndRegister %s \nto %s \nwith message: %s' % (lfn, se, res['Message']))
					upload_status=2
				elif res['Value']['Failed'].has_key(lfn):
					gLogger.error('Failed to putAndRegister %s to %s' % (lfn, se))
					upload_status=2
				uploaded_files.update([f])

			

			else:	#{fgroup:folder}
				## if folder start with '/', then full path, else under user home/
				if folder.startswith('/'):
					dirname=folder
				else:
					userHome = getUserHome()
					if not userHome:
						gLogger.error('Failed to get user home')
						return 1
					dirname=os.path.join(userHome,folder)
			
				flist_raw=fgroup.split(',')
				flist=[]


				for f in flist_raw:
					l=glob.glob(os.path.join(source_location,f))
					l=[os.path.relpath(x,source_location) for x in l]
					flist+=l
				flist=list(set(flist))	# remove repeated items
			
				
				for f in flist:
					lfn = os.path.join(dirname,f)
			
					fcc = FileCatalogClient()
					dm = DataManager(['FileCatalog'])
			
					print('registering file: LFN={0}, source={1}, SE={2}'.format(lfn,os.path.join(source_location,f),se))
					if f not in uploaded_files:
						res = dm.putAndRegister(lfn, os.path.join(source_location,f), se, overwrite=overwrite)
					if not res['OK']:
						if f not in ff:
							gLogger.error('Failed to putAndRegister %s \nto %s \nwith message: %s' % (lfn, se, res['Message']))
							upload_status=2
						else:
							gLogger.error('Allowed failure to putAndRegister %s \nto %s \nwith message: %s.' % (lfn, se, res['Message']))
					
					elif res['Value']['Failed'].has_key(lfn):
						if f not in ff:
							gLogger.error('Failed to putAndRegister %s to %s' % (lfn, se))
							upload_status=2
						else:
							gLogger.error('Allowed failure to putAndRegister %s to %s.' % (lfn, se))
					uploaded_files.update([f])

	return upload_status


if __name__ == '__main__':
	exit(main())
