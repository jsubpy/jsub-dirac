import os
import subprocess
import json

DIRAC_JOBVAR_DIR = os.path.dirname(os.path.realpath(__file__))


#DIRAC environment is required for using this jobvar
class FindLfns(object):
	def __init__(self, param):
		self.__path = param.get('path')
		self.__SE = param.get('SE')
		self.__metaspec = param.get('metaspec')

		cmd = [os.path.join(DIRAC_JOBVAR_DIR, 'script', 'find_lfns.sh')]
		cmd+= [' Path=%s '%self.__path]		
		if self.__SE:
			cmd+= [' SE=%s '%self.__SE]
		if self.__metaspec:
			cmd+= [' %s'%self.__metaspec]
		
		try:
			lfnlist = subprocess.check_output(cmd) #binary
			lfnlist=(lfnlist.decode('UTF-8')).split('\n')[:-1]	#binary to str list
			self.__lfns=lfnlist
		except:
			self.__lfns=[]
		


	def next(self):
		if len(self.__lfns)==0:
			raise StopIteration

		path = os.path.join(self.__path,self.__lfns.pop(0))	

		return {'value':path}


	def length(self):
		return None
