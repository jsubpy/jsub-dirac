#!/bin/sh


if [ -z "$DIRAC" ]; then
  >&2 echo 'DIRAC environment is not set'
  exit 1
fi

source "$DIRAC/bashrc"


dirac-wms-job-get-output $@
