#!/bin/bash
date
if [ $# == 0 ]; then
    echo 'USAGE: $ time_disp_py.sh {script.py} {argument} {argument}...'
    exit 1
fi
python $@
echo $SECONDS 'sec.'
