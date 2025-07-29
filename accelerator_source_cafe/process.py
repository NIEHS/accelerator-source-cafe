import logging

from accelerator_source_cafe.cafe_config import CafeConfig
from accelerator_core.workflow.accel_source_ingest import IngestSourceDescriptor
from cafe_accel_source import CafeAccelSource
from cafe_crosswalk import CafeCrosswalk

from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"
)

logger = logging.getLogger(__name__)


def main(api_url: str, api_headers: dict, params: dict, type: str, submitter_name: str, submitter_email: str):
    logger.info("process.py::main()")
    # Create an IngestSourceDescriptor instance and populate metadata
    ingest_source_descriptor = IngestSourceDescriptor()
    ingest_source_descriptor.type = type
    ingest_source_descriptor.submitter_name = submitter_name
    ingest_source_descriptor.submitter_email = submitter_email
    ingest_source_descriptor.submit_date = '2021-01-01'

    # Initialize the specific ingest component
    cafe_accel_source = CafeAccelSource(ingest_source_descriptor)

    # Ingest the data
    ingest_results = cafe_accel_source.ingest({'api_url': api_url, 'api_headers': api_headers, 'params': params})


if __name__ == '__main__':

    cafe_properties = '../tests/test_resources/cafe.properties'
    cafe_config = CafeConfig(cafe_properties)
    api_url = cafe_config.cafe_properties["cafe_endpoint"]
    api_headers = cafe_config.build_request_headers_json()
    params = {
        'q': '*',  # Query string
        'subtree': 'CAFE',  # Subtree filter
        'type': 'dataset'  # Specify dataset type
    }
    type = 'CHORDS'
    submitter_name = 'John Doe'
    submitter_email = 'john.doe@test.com'
    main(api_url=api_url, api_headers=api_headers, params=params, type=type, submitter_name=submitter_name, submitter_email=submitter_email)

