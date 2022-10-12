#!/bin/sh

if [ -z "$DIRACOS" ]; then
  >&2 echo 'DIRAC environment is not set!'
  exit 1
fi


source "$DIRACOS/diracosrc"

cur_dir=$(dirname $0)

"$cur_dir/dirac-run.py" "$@"
