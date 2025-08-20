import requests
import uuid
import json
import os

from accelerator_core.utils.logger import setup_logger
from accelerator_core.utils.xcom_utils import XcomPropsResolver
from accelerator_source_cafe.cafe_config import CafeConfig
from accelerator_core.workflow.accel_source_ingest import AccelIngestComponent
from accelerator_core.workflow.accel_source_ingest import IngestSourceDescriptor
from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)

logger = setup_logger("accelerator")

class CedarAccelParameters():

    def __init__(self, accel_params):
        self.accel_params = accel_params

class CafeAccelSource(AccelIngestComponent):
    """
    A subclass of AccelIngestComponent that implements the ingest process for Data.gov
    """

    def __init__(self, ingest_source_descriptor: IngestSourceDescriptor, xcom_props_resolver:XcomPropsResolver):
        super().__init__(ingest_source_descriptor, xcom_props_resolver)


    def ingest_single(self, identifier:str, additional_parameters: dict) -> IngestPayload:
        """
        Ingest a single document based on its CAFE ID.

        :param identifier: CAFE document identifier
        :param additional_parameters: Additional parameters for this ingest component
        """
        pass


    def ingest(self, additional_parameters: dict) -> IngestPayload:
        """
        Ingest data from a data.gov and return the result in an IngestResult object.

        :param additional_parameters: Dictionary containing parameters such as the api url and token
        :return: IngestResult with the parsed data from the spreadsheet
        """
        logger.info("CafeAccelSource::ingest()")
        api_url = additional_parameters.get('api_url')
        api_headers = additional_parameters.get('api_headers')
        params = additional_parameters.get('params')

        if not api_url or not api_headers or not params:
            raise ValueError("API URL, headers and parameters must be provided")

        # Call the basic dataset search method
        basic_search_result = self.basic_cafe_search(api_url=api_url, api_headers=api_headers, params=params)
        doi_list = self.extract_doi_list_from_dataset_result(basic_search_result)

        # Extract metadata for each doi and store it as a json file
        datasets = []
        for doi in doi_list:
            persistent_id = doi
            dataset_metadata = self.get_dataset_metadata_by_persistent_id(api_url=api_url, api_headers=api_headers,persistent_id=persistent_id)
            if isinstance(dataset_metadata, dict):
                datasets.append(dataset_metadata)
            else:
                logger.warning("Invalid dataset metadata returned for DOI: %s", persistent_id)

        if len(datasets) < 1:
            return None
        else:
            # self.dump_data(datasets=datasets)
            
            # Create an IngestResult object
            ingest_result = IngestPayload(self.ingest_source_descriptor)
            ingest_result.payload = datasets
            ingest_result.ingest_successful = True
            return ingest_result


    @staticmethod
    def basic_cafe_search(api_url: str = None, api_headers: dict = None, params: dict = None):
        """
        Perform a basic search on the Cafe Dataverse repository
        """
        logger.info('CafeAccelSource::basic_cafe_search()')
        api_url = api_url + "/api/search"
        api_headers = api_headers

        params['per_page'] = 500
        search_result = requests.get(api_url, headers=api_headers, params=params)
        if search_result.status_code == 200:
            search_result = search_result.json()
            logger.info('Data: %s', search_result)
            return search_result
        else:
            logger.info(f"Failed to retrieve data. Status code: {search_result.status_code}")

    @staticmethod
    def get_dataset_metadata_by_persistent_id(api_url: str = None, api_headers: dict = None, persistent_id: str = None):
        """
        Retrieve the metadata for a dataset by its persistent ID
        """
        logger.info('CafeAccelSource::get_dataset_metadata_by_persistent_id()')
        api_url = api_url + "/api/datasets/:persistentId/versions/:latest"
        api_headers = api_headers

        dataset_metadata = requests.get(api_url, headers=api_headers, params={'persistentId': persistent_id})
        if dataset_metadata.status_code == 200:
            dataset_metadata = dataset_metadata.json()
            logger.info('Data: %s', dataset_metadata)
            return dataset_metadata
        else:
            logger.info(f"Failed to retrieve data. Status code: {dataset_metadata.status_code}")
            return None

    @staticmethod
    def extract_doi_list_from_dataset_result(dataset_result):
        """
        Extract the DOIs from a dataset result
        """
        logger.info('CafeAccelSource::extract_doi_list_from_dataset_result()')
        doi_list = []

        if not dataset_result:
            logger.warning("No dataset result provided (None received).")
            return doi_list
        try:
            for dataset in dataset_result['data']['items']:
                doi_list.append(dataset['global_id'])
        except (KeyError, TypeError) as e:
            logger.error("Error while extracting DOIs: %s", e)

        return doi_list

    @staticmethod
    def dump_data(datasets: list):
        """
        Dump the data into JSON files in a specified folder.
        :param datasets: A list of datasets
        :return:
        """
        logger.info("DataGovAccelSource::dump_data()")
        # Folder containing JSON files
        folder_path = "../tests/test_resources/cafe_dump_05_07_2025"

        # Check if the folder exists, if not, create it
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            logger.info(f"Folder created: {folder_path}")
        else:
            logger.info(f"Folder already exists: {folder_path}")

        for item in datasets:
            temp_dataset_id = item['data']['datasetId']
            logger.info('Dataset Metadata: %s', item)
            filename = os.path.join(folder_path, f"{temp_dataset_id}.json")
            with open(filename, 'w') as f:
                json.dump(item, f, indent=4)




