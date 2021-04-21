#!/bin/bash

args=$@
set -- ""

source /opt/nec/ve/nlc/2.1.0/bin/nlcvars.sh
python3 sum.py $args
