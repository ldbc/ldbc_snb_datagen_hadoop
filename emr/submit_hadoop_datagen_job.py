#!/usr/bin/env python3

import boto3
from os import path
from datetime import datetime
import pprint
import csv
import re
import __main__
import argparse
import sys
from math import ceil


class KeyValue(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, dict())

        for value in values:
            # split it into key and value
            key, value = value.split('=', maxsplit=1)
            # assign into dictionary
            getattr(namespace, self.dest)[key.strip()] = value


def ask_continue(message):
    print(message)
    resp = None
    inp = input("Continue? [Y/N]: ").lower()
    while resp is None:
        if inp == 'y' or inp == 'yes':
            resp = True
        elif inp == 'n' or inp == 'no':
            resp = False
        else:
            inp = input("Please answer Y/N: ").lower()
    return resp


min_num_workers = 1
max_num_workers = 1000
min_num_threads = 1

defaults = {
    'bucket': 'ldbc-snb-datagen-store',
    'use_spot': True,
    'master_instance_type': 'r6gd.2xlarge',
    'instance_type': 'r6gd.2xlarge',
    'sf_per_executor': 3e3,
    'sf_per_partition': 10,
    'az': 'us-east-2c',
    'yes': False,
    'ec2_key': None,
    'emr_release': 'emr-6.15.0'
}

pp = pprint.PrettyPrinter(indent=2)

dir = path.dirname(path.realpath(__file__))
ec2info_file = 'Amazon EC2 Instance Comparison.csv'


with open(path.join(dir, ec2info_file), mode='r') as infile:
    reader = csv.DictReader(infile)
    ec2_instances = [dict(row) for row in reader]


def get_instance_info(instance_type):
    def parse_vcpu(col):
        return int(re.search(r'(\d+) .*', col).group(1))

    def parse_mem(col):
        return int(re.search(r'(\d+).*', col).group(1))

    vcpu = next((parse_vcpu(i['vCPUs']) for i in ec2_instances if i['API Name'] == instance_type), None)
    mem = next((parse_mem(i['Memory']) for i in ec2_instances if i['API Name'] == instance_type), None)

    if vcpu is None or mem is None:
        raise Exception(f'unable to find instance type `{instance_type}`. If not a typo, reexport `{ec2info_file}` from ec2instances.com')

    return {'vcpu': vcpu, 'mem': mem}


def submit_datagen_job(name,
                       sf,
                       bucket,
                       jar,
                       instance_type,
                       executors,
                       sf_per_executor,
                       partitions,
                       sf_per_partition,
                       master_instance_type,
                       az,
                       emr_release,
                       yes,
                       conf,
                       **kwargs
                       ):

    is_interactive = (not yes) and hasattr(__main__, '__file__')

    emr = boto3.client('emr')

    ts = datetime.utcnow()
    ts_formatted = ts.strftime('%Y%m%d_%H%M%S')

    jar_url = f's3://{bucket}/jars/{jar}'

    results_url = f's3://{bucket}/results/{name}'
    run_url = f'{results_url}/runs/{ts_formatted}'

    spark_config = {
        'maximizeResourceAllocation': 'true'
    }

    if executors is None:
        executors = max(min_num_workers, min(max_num_workers, ceil(sf / sf_per_executor)))

    if partitions is None:
        partitions = max(min_num_threads, ceil(sf / sf_per_partition))

    market = 'SPOT' if use_spot else 'ON_DEMAND'

    ec2_key_dict = {'Ec2KeyName': ec2_key} if ec2_key is not None else {}

    job_flow_args = {
        'Name': f'{name}_{ts_formatted}',
        'LogUri': f's3://{bucket}/logs/emr',
        'ReleaseLabel': emr_release,
        'Applications': [
            {'Name': 'hadoop'},
            {'Name': 'ganglia'}
        ],
        'Configurations': [
        ],
        'Instances': {
            'InstanceGroups': [
                {
                    'Name': "Driver node",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1,
                },
                {
                    'Name': "Worker nodes",
                    'Market': market,
                    'InstanceRole': 'CORE',
                    'InstanceType': instance_type,
                    'InstanceCount': executors,
                }
            ],
            **ec2_key_dict,
            'Placement': {'AvailabilityZone': az},
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        'JobFlowRole': 'EMR_EC2_DefaultRole',
        'ServiceRole': 'EMR_DefaultRole',
        'VisibleToAllUsers': True,
        'Steps': [
            {
                'Name': 'Run LDBC SNB Datagen',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Properties': [],
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', '--class', 'ldbc.snb.datagen.LdbcDatagen', jar_url,
                             '--output-dir', build_dir,
                             '--scale-factor', str(sf),
                             '--num-threads', str(partitions)
                             # *passthrough_args
                             ]
                }

            },
            {
                'Name': 'Save output',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Properties': [],
                    'Jar': 'command-runner.jar',
                    'Args': ['s3-dist-cp',
                             '--src', f'hdfs://{build_dir}',
                             '--dest', f'{run_url}/social_network',
                             *(['--srcPattern', copy_filter] if not copy_all else [])
                             ]
                }
            }]
    }

    if is_interactive:
        job_flow_args_formatted = pp.pformat(job_flow_args)
        if not util.ask_continue(f'Job parameters:\n{job_flow_args_formatted}'):
            return

    emr.run_job_flow(**job_flow_args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Submit a Hadoop Datagen job to AWS EMR')
    parser.add_argument('--name',
                        type=str,
                        help='name')
    parser.add_argument('--scale-factor', type=int,
                        help='scale factor (used to calculate cluster size)')
    parser.add_argument('--az',
                        default=defaults['az'],
                        help=f'Cluster availability zone. Default: {defaults["az"]}')
    parser.add_argument('--bucket',
                        default=defaults['bucket'],
                        help=f'LDBC SNB Datagen storage bucket. Default: {defaults["bucket"]}')
    parser.add_argument('--instance-type',
                        default=defaults['instance_type'],
                        help=f'Override the EC2 instance type used for worker nodes. Default: {defaults["instance_type"]}')
    parser.add_argument('--ec2-key',
                        default=defaults['ec2_key'],
                        help='EC2 key name for SSH connection')
    parser.add_argument('--jar',
                        required=True,
                        help='LDBC SNB Datagen library JAR name')
    parser.add_argument('--emr-release',
                        default=defaults['emr_release'],
                        help='The EMR release to use. E.g. emr-6.6.0')
    parser.add_argument('-y', '--yes',
                        default=defaults['yes'],
                        action='store_true'
                        help='Assume \'yes\' for prompts')
    # copy_args = parser.add_mutually_exclusive_group()
    # executor_args=parser.add_mutually_exclusive_group()
    # executor_args.add_argument("--executors",
    #                            type=int,
    #                            help=f"Total number of Spark executors."
    #                            )
    # executor_args.add_argument("--sf-per-executor",
    #                            type=float,
    #                            default=defaults['sf_per_executor'],
    #                            help=f"Number of scale factors per Spark executor. Default: {defaults['sf_per_executor']}"
    #                            )
    # partitioning_args = parser.add_mutually_exclusive_group()
    # partitioning_args.add_argument("--partitions",
    #                                type=int,
    #                                help=f"Total number of Spark partitions to use when generating the dataset."
    #                                )
    # partitioning_args.add_argument("--sf-per-partition",
    #                                type=float,
    #                                default=defaults['sf_per_partition'],
    #                                help=f"Number of scale factors per Spark partitions. Default: {defaults['sf_per_partition']}"
    #                                )

    print("parse args")
    args = parser.parse_args()
    print(args.__dict__)
    print("/parse args")

    submit_datagen_job(master_instance_type=defaults['master_instance_type'], **args.__dict__)
