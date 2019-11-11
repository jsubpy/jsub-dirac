#!/usr/bin/env python

import sys
import json

try:
    from DIRAC.Core.Base import Script
except ImportError as e:
    sys.stdout.write('DIRAC client not properly setup: %s\n' % e)
    sys.exit(1)

Script.setUsageMessage('Run DIRAC sub command')
Script.registerSwitch('', 'name=', 'Job name')
Script.registerSwitch('', 'job-group=', 'Job group')
Script.registerSwitch('', 'sub-ids=', 'Job sub id list')
Script.registerSwitch('', 'input-sandbox=', 'Input sandbox')
Script.registerSwitch('', 'output-sandbox=', 'Output sandbox')
Script.registerSwitch('', 'executable=', 'Job executable')
Script.registerSwitch('', 'site=', 'Job sites')
Script.registerSwitch('', 'banned-site=', 'Job banned sites')
Script.parseCommandLine(ignoreErrors=False)

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.Job import Job


def submit(name, job_group, sub_ids, input_sandbox, output_sandbox, executable, site=None, banned_site=None):
    job_names = [name + '-' + sub_id for sub_id in sub_ids]

    j = Job()
    j.setName(name)
    j.setExecutable(executable)
    j.setParameterSequence('JobName', job_names, addToWorkflow=True)
    j.setParameterSequence('arguments', sub_ids, addToWorkflow=True)

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

    dirac = Dirac()
    result = dirac.submitJob(j)
    if not result['OK']:
        sys.stdout.write('DIRAC job submit error: %s\n' % result['Message'])
        sys.exit(1)

    final_result = {}
    for sub_id, dirac_id in zip(sub_ids, result['Value']):
        final_result[sub_id] = dirac_id
    return final_result


def getListArg(arg):
    if not arg:
        return []
    return arg.split(',')

def main():
    args = Script.getPositionalArgs()
    switches = Script.getUnprocessedSwitches()

    name = 'jsub'
    job_group = 'jsub-job'
    sub_ids = []
    input_sandbox = []
    output_sandbox = []
    executable = ''
    site = None
    banned_site = None

    for k, v in switches:
        if k == 'name':
            name = v
        elif k == 'job-group':
            job_group = v
        elif k == 'sub-ids':
            sub_ids = getListArg(v)
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

    if args[0] == 'submit':
        result = submit(name, job_group, sub_ids,
                        input_sandbox, output_sandbox, executable, site, banned_site)
        print(json.dumps(result))

    return 0


if __name__ == '__main__':
    exit(main())
