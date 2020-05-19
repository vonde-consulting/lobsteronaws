import pandas as pd


def prepare_stocks_task_file(output_file: str, stocks: list, start_date: str, end_date: str, level: int) -> str:
    with open(output_file, mode='w') as _f:
        _f.write("stock, startDate, endDate, level \n")
        for _s in stocks:
            _f.write(f"{_s}, {start_date}, {end_date}, {level} \n")

    return output_file


def prepare_task_file_by_template(output_file: str, template_file: str, start_date=None, end_date=None,
                                  level=None) -> str:
    _old_task = pd.read_csv(template_file, header=0, skipinitialspace=True)
    _stocks = _old_task['stock'].values
    _start_date = _old_task['startDate'].iloc[0]
    _end_date = _old_task['endDate'].iloc[0]
    _level = _old_task['level'].iloc[0]
    if not (start_date is None):
        _start_date = start_date

    if not (end_date is None):
        _end_date = end_date
    if not (level is None):
        _level = level

    return prepare_stocks_task_file(output_file, _stocks, _start_date, _end_date, _level)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Construct order book using LOBSTER engine")
    parser.add_argument('-t', '--template_file', required=True,
                        help="Template file such as s3://demo-ordermessage-lobsterdata-com/NASDAQ100-2019-12-30.txt")
    parser.add_argument('-o', '--output_file', required=True,
                        help="Output file name")
    parser.add_argument('-s', '--start_date', type=str, help="Start date, format yyyy-mm-dd.")
    parser.add_argument('-e', '--end_date', type=str, help="End date, format yyyy-mm-dd.")
    parser.add_argument('-l', '--level', type=int, help="Level of the order book. ")

    args = parser.parse_args()

    template_file = args.template_file
    output_file = args.output_file
    start_date = args.start_date
    end_date = args.end_date
    level = args.level
    prepare_task_file_by_template(output_file, template_file, start_date, end_date, level)


if __name__ == "__main__":
    main()
