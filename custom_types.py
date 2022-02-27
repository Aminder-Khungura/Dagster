import requests
import csv
from dagster import job, op, get_dagster_logger, DagsterType, In, Out


def is_list_of_dicts(_, value):
    return isinstance(value, list) and all(isinstance(element, dict) for element in value)


SimpleDataFrame = DagsterType(
    name="SimpleDataFrame",
    type_check_fn=is_list_of_dicts,
    description="A naive representation of a data frame, e.g., as returned by csv.DictReader.",)


@op(out=Out(SimpleDataFrame))
def download_csv():
    response = requests.get("https://docs.dagster.io/assets/cereal.csv")
    lines = response.text.split("\n")
    get_dagster_logger().info(f"Read {len(lines)} lines")
    return [row for row in csv.DictReader(lines)]


@op(ins={"cereals": In(SimpleDataFrame)})
def sort_by_calories(cereals):
    sorted_cereals = sorted(cereals, key=lambda cereal: int(cereal["calories"]))
    get_dagster_logger().info(f'Most caloric cereal: {sorted_cereals[-1]["name"]}')


@job
def custom_type_job():
    sort_by_calories(download_csv())