#!/bin/bash

step=10

for (( i = 0; i < 60; i=(i+step) )); do
    python $AIRFLOW_HOME/plugins/dagx/dagx_converter.py
    sleep $step
done

exit 0