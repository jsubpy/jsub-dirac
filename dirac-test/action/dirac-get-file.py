#!/usr/bin/env python

from DIRAC import S_OK, S_ERROR, gLogger, exit
from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC.Interfaces.API.Dirac import Dirac


filepath = '/cefs/data/stdhep/CEPC240/higgs/exclusive/E240.Pn3n3h_cc.e0.p0.whizard195/n3n3h_cc.e0.p0.00001.stdhep'


def main():
    lfn = '/cepc/lustre-ro/' + filepath

    dirac = Dirac()

    result = dirac.getFile(lfn)
    if not result['OK']:
        gLogger.error('Download file error: %s' % result['Message'])
        return 2

    return 0


if __name__ == '__main__':
    exit(main())
