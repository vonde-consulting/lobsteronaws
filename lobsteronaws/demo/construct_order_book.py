import tempfile

from lobsteronaws.aws.emr import LobsterEMR


def prepare_instance_group_json_file():
    _file_name = tempfile.NamedTemporaryFile().name + ".json"
    with open(_file_name, 'w') as f:
        f.write("""
[
  {
    "InstanceCount": 1,
    "BidPrice": "10.000",
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 32,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 2
        }
      ]
    },
    "InstanceGroupType": "MASTER",
    "InstanceType": "c4.xlarge",
    "Name": "Master - 1"
  },
  {
    "InstanceCount": 2,
    "BidPrice": "10.000",
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 64,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 1
        }
      ]
    },
    "InstanceGroupType": "CORE",
    "InstanceType": "m5.4xlarge",
    "Name": "Core-BookConstruct-Demo",
    "AutoScalingPolicy":
    {
     "Constraints":
      {
       "MinCapacity": 1,
       "MaxCapacity": 10
      },
     "Rules":
     [
      {
       "Name": "Default-scale-out",
       "Description": "Replicates the default scale-out rule in the console for YARN memory.",
       "Action":{
        "SimpleScalingPolicyConfiguration":{
          "AdjustmentType": "CHANGE_IN_CAPACITY",
          "ScalingAdjustment": 1,
          "CoolDown": 300
        }
       },
       "Trigger":{
        "CloudWatchAlarmDefinition":{
          "ComparisonOperator": "LESS_THAN",
          "EvaluationPeriods": 1,
          "MetricName": "YARNMemoryAvailablePercentage",
          "Namespace": "AWS/ElasticMapReduce",
          "Period": 300,
          "Threshold": 10,
          "Statistic": "AVERAGE",
          "Unit": "PERCENT",
          "Dimensions":[
             {
               "Key" : "JobFlowId",
               "Value" : "${emr.clusterId}"
             }
          ]
        }
       }
      }
     ]
   }
  }
]
        """)
        return _file_name


def prepare_steps_json_file(s3_request_file, s3_input_path, s3_output_path,
                            s3_jar="s3://bookconstructor-lobsterdata-com/com-lobsterdata-bookconstructor_2.11-0.1.jar",
                            output_format="parquet", num_partitions=0):
    _file_name = tempfile.NamedTemporaryFile().name + ".json"
    with open(_file_name, mode='w') as f:
        f.write(f"""
    [
        {{
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--driver-memory",
                "6G",
                "--driver-cores",
                "3",
                "--executor-memory",
                "11G",
                "--executor-cores",
                "3",
                "--class",
                "com.lobsterdata.app.ConstructBook",
                "{s3_jar}",
                "{s3_request_file}",
                "{s3_input_path}",
                "{s3_output_path}",
                "{output_format}",
                "{num_partitions}"
            ],
            "Type": "CUSTOM_JAR",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "Jar": "command-runner.jar",
            "Properties": "",
            "Name": "Lobster Book Construction"
        }}
    ]
        """)

    return _file_name


def main():
    import argparse
    import os
    dir_path = os.path.dirname(os.path.realpath(__file__))
    parser = argparse.ArgumentParser(description="Construct order book using LOBSTER engine")
    parser.add_argument('-t', '--task_file', required=True, help="""S3 task file. 
                            Example file: s3://demo-ordermessage-lobsterdata-com/NASDAQ100-2019-12-30.txt""")
    parser.add_argument('-k', '--key_name', required=True, help="EMR pair key name")
    parser.add_argument('-i', '--input_path', required=True,
                        help="Input path. The default is s3://demo-ordermessage-lobsterdata-com")
    parser.add_argument('-o', '--output_path', required=True, help="Output path. Required. S3 location.")
    parser.add_argument('-g', '--instance_groups',
                        default=os.path.join(dir_path, "config", "construct_book_instance_groups.json"),
                        help="Instance group configure JSON file")
    parser.add_argument('-j', '--jar_file',
                        default="s3://bookconstructor-lobsterdata-com/com-lobsterdata-bookconstructor_2.11-0.1.jar",
                        help="The jar file contains LOBSTER engine. The default location is " +
                             "s3://bookconstructor-lobsterdata-com/com-lobsterdata-bookconstructor_2.11-0.1.jar")
    parser.add_argument('-f', '--output_format', default='parquet',
                        help="Output format. It could be parquet or csv. The default is parquet")
    parser.add_argument('-r', '--region', default="eu-west-2", help="EMR region. Default is eu-west-2")
    parser.add_argument('-p', '--credential_profile', default="default",
                        help="AWS credential profile. Default is 'default'")
    parser.add_argument('-n', '--num_partitions', type=int,
                        default=10,
                        help="""The number of partitions for output files. 
                        Lobster engine always partitions the output by symbols. If choose 0, the number of the 
                        partition will be automatically determined by Spark. """)

    args = parser.parse_args()

    task_file = args.task_file
    instance_groups = args.instance_groups
    input_path = args.input_path
    output_path = args.output_path
    output_format = args.output_format
    num_partitions = args.num_partitions
    jar_file = args.jar_file
    key_name = args.key_name
    region = args.region
    profile = args.credential_profile
    log_uri = output_path + "/log"
    confirmation = input(f"""
    You are about to start the following task on AWS:
    * Task file: {task_file}
    * Instance groups: {instance_groups}
    * Input path: {input_path}
    * Output path: {output_path}
    * Output format: {output_format}
    * Number of partition: {num_partitions} (0 means using Spark default number)
    * Lobster engine jar file: {jar_file}
    * EMR pair key: {key_name}
    * Region: {region}
    * Log location: {log_uri}
    Please confirm (yes or no) >> """)
    if confirmation == 'yes':
        step_file = prepare_steps_json_file(task_file, input_path, output_path, jar_file,
                                            output_format, num_partitions)
        construction_emr = LobsterEMR(instance_groups, step_file, key_name, name='Lobster Book Construction',
                                      region=region, credential_profile=profile, log_uri=log_uri)
        construction_emr.create()


if __name__ == "__main__":
    main()
