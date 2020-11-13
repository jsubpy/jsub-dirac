#!/bin/sh


if [ -z "$DIRAC" ]; then
  >&2 echo 'DIRAC environment is not set'
  exit 1
fi

source "$DIRAC/bashrc"


echo Executing dirac-dms-find-lfns to generate subjob lists...
eval dirac-dms-find-lfns $@
