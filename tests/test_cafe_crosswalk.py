import os
import json
import logging
import shutil
import unittest

from accelerator_core.utils.xcom_utils import DirectXcomPropsResolver, XcomUtils
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


    def test_cafe_crosswalk(self):
        # Create an IngestSourceDescriptor instance and populate metadata
        temp_dirs_path = "test_resources/temp_dirs"
        runid = "test_cafe_crosswalk_key_dataset"
        item_id = "test_crosswalk_key_dataset_item"
        path = os.path.join(temp_dirs_path, runid)

        if os.path.exists(path):
            shutil.rmtree(path)

        try:
            xcom_props_resolver = DirectXcomPropsResolver(temp_files_supported=True, temp_files_location=temp_dirs_path)
            xcom_utils = XcomUtils(xcom_props_resolver)

            # Create an IngestSourceDescriptor instance and populate metadata
            ingest_source_descriptor = IngestSourceDescriptor()
            ingest_source_descriptor.ingest_identifier = "test"
            ingest_source_descriptor.ingest_item_id = item_id
            ingest_source_descriptor.ingest_identifier = runid
            ingest_source_descriptor.type = type
            ingest_source_descriptor.submitter_name = "submitter_name"
            ingest_source_descriptor.submitter_email = "submitter_email"
            ingest_source_descriptor.schema_version = "1.0.1"
            #ingest_source_descriptor.submit_date = '2021-01-01'

            # Create an IngestPayload object
            ingest_payload = IngestPayload(ingest_source_descriptor)
            ingest_payload.payload_inline = False

            file_path = 'test_resources/cafe_dump_05_07_2025/39154.json'

            with open(file_path, "r", encoding="utf-8") as f:
                filename = os.path.basename(file_path)
                contents_json = json.loads(f.read())
                logger.info(f"Loaded JSON from {filename}: {contents_json}")

                stored_path = xcom_utils.store_dict_in_temp_file(item_id, contents_json, runid)
                ingest_payload.payload_path.append(stored_path)

                # Transform the data using a crosswalk
                crosswalk = CafeCrosswalk(xcom_props_resolver)
                actual = crosswalk.transform(ingest_result=ingest_payload)
                self.assertTrue(actual)

        except json.JSONDecodeError:
            print(f"Skipping invalid JSON: {filename}")

if __name__ == '__main__':
    unittest.main()