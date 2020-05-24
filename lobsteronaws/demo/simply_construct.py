import tempfile
from datetime import date, timedelta, datetime
from lobsteronaws.aws.emr import *


def get_range_weekdays(start_date: date, end_date: date):
    while start_date <= end_date:
        if start_date.isoweekday() < 6:
            yield start_date
        start_date += timedelta(days=1)


def steps_file(s3_stock_file: str, book_level: int, start_date: str, end_date: str,
               s3_input_path: str, s3_output_path: str, s3_jar, output_format, num_partitions,
               region:str = "eu-west-2") -> str:
    _file_name = tempfile.NamedTemporaryFile().name + ".json"
    _start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    _end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    _weekdays = get_range_weekdays(_start_date, _end_date)
    _steps = [f"""{{
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "client",
                    "--driver-memory",
                    "6G",
                    "--driver-cores",
                    "3",
                    "--executor-memory",
                    "18G",
                    "--executor-cores",
                    "5",  
                    "--class",
                    "com.lobsterdata.app.SimpleConstructor",
                    "{s3_jar}",
                    "{s3_stock_file}",
                    "{s3_input_path}",
                    "{s3_output_path}",
                    "{_a_day.strftime('%Y-%m-%d')}",
                    "{book_level}",
                    "{output_format}",
                    "{num_partitions}",
                    "{region}"
                ],
                "Type": "CUSTOM_JAR",
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "Jar": "command-runner.jar",
                "Properties": "",
                "Name": "Construct {_a_day.strftime('%Y-%m-%d')}"
            }}""" for _a_day in _weekdays
              ]

    with open(_file_name, mode='w') as f:
        f.write("[ \n" + ",".join(_steps) + "\n]")

    return _file_name


def main():
    import argparse
    import os
    from datetime import datetime

    dir_path = os.path.dirname(os.path.realpath(__file__))
    parser = argparse.ArgumentParser(description="Construct order book using LOBSTER engine")
    parser.add_argument('-t', '--stock_file', required=True,
                        help="Local stocks file. This file will be copied to s3 as outputpath/<time>/stocks.csv. " +
                             "Template file: demo/NASDAQ100.txt")
    parser.add_argument('-l', '--book_level', type=int, required=True, help="Order book leve. ")
    parser.add_argument('-s', '--start_date', type=str, required=True, help="Start date. Format yyyy-mm-dd.")
    parser.add_argument('-e', '--end_date', type=str, required=True, help="End date. format yyyy-mm-dd.")
    parser.add_argument('-i', '--input_path', required=True,
                        help="Input path. S3 location. E.g. s3://demo-ordermessage-lobsterdata-com")
    parser.add_argument('-o', '--output_path', required=True, help="Output path. Required. S3 location.")
    parser.add_argument('-f', '--output_format', default='parquet',
                        help="Output format. It could be parquet or csv. The default is parquet")
    parser.add_argument('-n', '--num_partitions', type=int,
                        default=5,
                        help="""The number of partitions for output files. 
                        Lobster engine always partitions the output by symbols. If choose 0, the number of the 
                        partition will be automatically determined by Spark. Default is 5 """)
    parser.add_argument('-j', '--jar_file',
                        default="s3://bookconstructor-lobsterdata-com/com-lobsterdata-bookconstructor_2.11-0.1.jar",
                        help="The jar file contains LOBSTER engine. The default location is " +
                             "s3://bookconstructor-lobsterdata-com/com-lobsterdata-bookconstructor_2.11-0.1.jar")
    parser.add_argument('-k', '--key_name', type=str, required=True, help="EMR pair key name. Without .pem")
    parser.add_argument('-g', '--instance_groups',
                        default=os.path.join(dir_path, "config", "construct_book_instance_groups.json"),
                        help="Instance group configure JSON file. Local file. " +
                             " Default is demo/config/construct_book_instance_groups.json")
    parser.add_argument('-r', '--region', default="eu-west-2", help="EMR region. Default is eu-west-2")
    parser.add_argument('-b', '--bucket_region', default="eu-west-2", help="S3 bucket region. Default is eu-west-2")
    parser.add_argument('-p', '--credential_profile', default="default",
                        help="AWS credential profile. Default is 'default'")

    args = parser.parse_args()
    stock_file = args.stock_file
    book_level = args.book_level
    start_date = args.start_date
    end_date = args.end_date
    input_path = args.input_path
    output_path = args.output_path
    output_format = args.output_format
    num_partitions = args.num_partitions
    jar_file = args.jar_file
    key_name = args.key_name
    instance_groups = args.instance_groups
    region = args.region
    bucket_region = args.bucket_region
    profile = args.credential_profile
    log_uri = output_path + "/log"
    stock_file_s3 = output_path + "/stocks.txt"
    confirmation = input(f"""
    You are about to start the following task on AWS:
    * Local stock file: {stock_file}
    * Stock file S3 upload location: {stock_file_s3}
    * Order book level: {book_level}
    * Start date: {start_date}
    * End date: {end_date}
    * Input path: {input_path}
    * Output path: {output_path}
    * Output format: {output_format}
    * Number of partition: {num_partitions} (0 means using Spark default number)
    * Lobster engine jar file: {jar_file}
    * EMR pair key name: {key_name}
    * Instance group file: {instance_groups}
    * EMR Region: {region}
    * Output bucket region: {bucket_region}
    * Log location: {log_uri}
    * Credential profile: {profile}
    Please confirm (yes or no) >> """)
    if confirmation == 'yes':
        upload_file_to_s3(stock_file, stock_file_s3)
        step_file = steps_file(stock_file_s3, book_level, start_date, end_date, input_path, output_path,
                               jar_file, output_format, num_partitions, region=bucket_region)
        construction_emr = LobsterEMR(instance_groups, step_file, key_name, name='Lobster Book Construction',
                                      region=region, credential_profile=profile, log_uri=log_uri)
        construction_emr.create()


if __name__ == "__main__":
    main()
