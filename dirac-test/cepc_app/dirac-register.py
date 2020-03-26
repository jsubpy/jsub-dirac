#!/usr/bin/env python

import os
import subprocess

_CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

cmd = [os.path.join(_CURRENT_DIR, 'script', 'register-run.sh')]
cmd += [
    '/cefs/data/stdhep/CEPC240/higgs/exclusive/E240.Pn3n3h_cc.e0.p0.whizard195/n3n3h_cc.e0.p0.00004.stdhep',
    '/cefs/data/stdhep/CEPC240/higgs/exclusive/E240.Pn3n3h_cc.e0.p0.whizard195/n3n3h_cc.e0.p0.00005.stdhep',
#    '/cefs/data/stdhep/CEPC240/higgs/exclusive/E240.Pn3n3h_cc.e0.p0.whizard195/n3n3h_cc.e0.p0.00003.stdhep',
]

ret = subprocess.call(cmd)
if ret != 0:
    print("Error")
