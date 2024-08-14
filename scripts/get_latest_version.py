import requests
import logging

from airflow.models import Variable
from bs4 import BeautifulSoup


def get_latest_version() -> str:
    biogrid_main_url = Variable.get('biogrid_main_url')
    response = requests.get(biogrid_main_url)

    if response.status_code != 200:
        logging.error('Failed to retrieve BioGRID main page')
        raise Exception('Failed to retrieve BioGRID main page')

    soup = BeautifulSoup(response.text, 'html.parser')
    version_links = soup.find_all('a', href=True)
    versions = [link.text.strip() for link in version_links if 'BIOGRID-' in link.text]

    if versions:
        latest_version = versions[0].split('-')[-1]
        return latest_version
    else:
        logging.error('No versions found on BioGRID website')
        raise Exception('No versions found on BioGRID website')
