import requests
import csv
from dagster import job, op, get_dagster_logger


@op
def download_cereals():
    response = requests.get('https://docs.dagster.io/assets/cereal.csv')
    lines = response.text.split('\n')
    return [row for row in csv.DictReader(lines)]  # returns a list


@op
def find_sugariest(cereals):
    sorted_by_sugar = sorted(cereals, key=lambda cereal: int(cereal['sugars']))  # lambda selects sugar for each dictionary element in list
    get_dagster_logger().info(f'{sorted_by_sugar[-1]["name"]} is the sugariest cereal')


@job
def serial():
    find_sugariest(download_cereals())