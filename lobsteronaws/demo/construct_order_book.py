import tempfile

from lobsteronaws.aws.emr import LobsterEMR, upload_file_to_s3

def prepare_steps_json_file(s3_request_file: str, s3_input_path: str, s3_output_path: str,
                            s3_jar="s3://bookconstructor-lobsterdata-com/com-lobsterdata-bookconstructor_2.11-0.1.jar",
                            output_format="parquet", num_partitions=0, executor_memory="11G") -> str:
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
                "{executor_memory}",
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
    from datetime import datetime

    dir_path = os.path.dirname(os.path.realpath(__file__))
    parser = argparse.ArgumentParser(description="Construct order book using LOBSTER engine")
    parser.add_argument('-t', '--task_file', required=True,
                        help="Local task file. This file will be copied to s3 as outputpath/<time>/task.csv. " + \
                             "Template file: s3://demo-ordermessage-lobsterdata-com/NASDAQ100-2019-12-30.txt")
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
    output_path = args.output_path + "/" + datetime.now().strftime("%Y%m%d%H%M%S")
    output_format = args.output_format
    num_partitions = args.num_partitions
    jar_file = args.jar_file
    key_name = args.key_name
    region = args.region
    profile = args.credential_profile
    log_uri = output_path + "/log"
    task_file_s3 = output_path + "/task.txt"
    confirmation = input(f"""
    You are about to start the following task on AWS:
    * Local task file: {task_file}
    * S3 task file location: {task_file_s3}
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
        upload_file_to_s3(task_file, task_file_s3)
        step_file = prepare_steps_json_file(task_file_s3, input_path, output_path, jar_file,
                                            output_format, num_partitions)
        construction_emr = LobsterEMR(instance_groups, step_file, key_name, name='Lobster Book Construction',
                                      region=region, credential_profile=profile, log_uri=log_uri)
        construction_emr.create()


if __name__ == "__main__":
    main()
