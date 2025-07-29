import os
import json
import logging
import unittest

from accelerator_source_cafe.cafe_crosswalk import CafeCrosswalk
from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s: %(filename)s:%(funcName)s:%(lineno)d: %(message)s"

)
logger = logging.getLogger(__name__)


class TestCafeCrosswalk(unittest.TestCase):
    # Create an IngestSourceDescriptor instance and populate metadata
    ingest_source_descriptor = IngestSourceDescriptor()
    ingest_source_descriptor.type = type
    ingest_source_descriptor.submitter_name = "submitter_name"
    ingest_source_descriptor.submitter_email = "submitter_email"
    ingest_source_descriptor.submit_date = '2021-01-01'

    file_path = 'test_resources/cafe_dump_05_07_2025/39154.json'

    with open(file_path, "r", encoding="utf-8") as f:
        filename = os.path.basename(file_path)
        try:
            data = json.load(f)
            logger.info(f"Loaded JSON from {filename}: {data}")

            # Create an IngestPayload object
            ingest_payload = IngestPayload(ingest_source_descriptor)
            ingest_payload.ingest_source_descriptor = ingest_source_descriptor
            ingest_payload.source_document_detail = 'cafe'
            ingest_payload.ingest_successful = False
            ingest_payload.payload_inline = False
            ingest_payload.payload = data

            # Transform the data using a crosswalk
            crosswalk = CafeCrosswalk()
            ingest_result = crosswalk.transform(ingest_result=ingest_payload)

        except json.JSONDecodeError:
            print(f"Skipping invalid JSON: {filename}")