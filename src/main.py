#!/usr/bin/python
import argparse
import os
import sys
import importlib
from pyspark.sql import SparkSession

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job', type=str, required=True, dest='job_name',
                        help="The name of the job module you want to run. (ex: poc will run job on jobs.poc package)")
    parser.add_argument('--master', type=str, required=False, dest='master', default='local[*]',
                        help="Spark driver")
    parser.add_argument('--job-args', nargs='*',
                        help="Extra arguments to send to the PySpark job (example: --job-args template=manual-email1")

    args = parser.parse_args()
    print("Called with arguments: %s" % args)

    spark = SparkSession.builder.master(args.master).appName(args.job_name).getOrCreate()
    job_module = importlib.import_module('jobs.%s' % args.job_name)

    sdf = job_module.extract(spark).transform(job_module.transform)
    job_module.load(sdf)