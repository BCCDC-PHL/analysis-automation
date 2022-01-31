#!/usr/bin/env python


import argparse
import json
import os
import re
import uuid

from prefect import task, Flow
from prefect.tasks.shell import ShellTask
from prefect.tasks.control_flow.filter import FilterTask

from datetime import time, timedelta
from prefect.schedules import IntervalSchedule


@task
def show_output(std_out):
    print(std_out)

@task
def list_subdirectories(parent_dir):
    subdirectories = []
    for f in os.scandir(parent_dir):
        if f.is_dir() and os.path.exists(os.path.join(f, 'upload_complete.json'):
            subdirectories.append(f.path)
    return subdirectories

@task
def build_workflow_command(run_dir):
    run_id = os.path.basename(run_dir)
    miseq_regex = '\d{6}_M\d{5}_\d+_\d{9}-[A-Z0-9]{5}'
    nextseq_regex = '\d{6}_VH\d{5}_\d+_[A-Z0-9]{9}'
    instrument_type = None
    if re.match(miseq_regex, run_id):
        instrument_type = 'miseq'
    elif re.match(nextseq_regex, run_id):
        instrument_type = 'nextseq'
    pipeline_name = 'BCCDC-PHL/routine-qc'
    pipeline_version = 'v0.3.2'
    nextflow_base_cmd  = 'nextflow run'
    outdir = os.path.join(run_dir, 'RoutineQC')
    work_dir_uuid = uuid.uuid4()
    work_dir = os.path.join(run_dir, 'work-' + str(work_dir_uuid))
    workflow_cmd = (f'{nextflow_base_cmd} {pipeline_name} -profile conda --cache ~/.conda/envs -r {pipeline_version}'
                    f'--run_dir {run_dir} --instrument_type {instrument_type} -work {work_dir} --outdir {outdir}')

    return workflow_cmd


filter_analyzed_runs = FilterTask(filter_func=lambda x: not os.path.exists(os.path.join(x, 'RoutineQC')))


@task
def build_workflow_task(cmd):
    task = ShellTask(command=cmd)
    return task


def build_flow(parent_dir, interval):
    check_for_runs_schedule = IntervalSchedule(
        interval=timedelta(seconds=interval)
    )

    with Flow("routine_qc", schedule=check_for_runs_schedule) as flow:
        subdirs = list_subdirectories(parent_dir)
        subdirs_to_analyze = filter_analyzed_runs(subdirs)
        cmd = build_workflow_command.map(subdirs_to_analyze)
        
    return flow


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', default=10)
    parser.add_argument('sequencer_output_dir')
    args = parser.parse_args()
    
    flow = build_flow(args.sequencer_output_dir, args.interval)
    flow.run()

if __name__ == '__main__':
    main()
