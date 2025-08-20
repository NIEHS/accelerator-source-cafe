from accelerator_core.workflow.accel_source_ingest import (
    IngestSourceDescriptor,
    IngestPayload,
)

from accelerator_core.utils.logger import setup_logger
from accelerator_core.utils.xcom_utils import XcomPropsResolver
from accelerator_core.workflow.crosswalk import Crosswalk

from accelerator_core.schema.models.accel_model import (
    AccelProgramModel,
    AccelProjectModel,
    AccelIntermediateResourceModel,
    build_accel_from_model,
    AccelResourceReferenceModel,
    AccelResourceUseAgreementModel,
    AccelPublicationModel,
    AccelDataResourceModel,
    AccelDataLocationModel,
    AccelGeospatialDataModel,
    AccelTemporalDataModel,
    AccelPopulationDataModel,
)
from accelerator_core.schema.models.base_model import (
    SubmissionInfoModel,
    TechnicalMetadataModel,
)
from airflow.providers.fab.auth_manager.models import metadata
from dill import citation

logger = setup_logger("accelerator-source-cafe")

class CafeCrosswalk(Crosswalk):
    """
    Crosswalks data from the ingest result into the appropriate format for downstream processing.
    """

    def __init__(self, xcom_props_resolver:XcomPropsResolver):
        """
        @param: xcom_properties_resolver XcomPropertiesResolver that can access
        handling configuration
        """

        super().__init__(xcom_props_resolver)

    def transform(self, ingest_result: IngestPayload) -> IngestPayload:
        """Convert raw data into a standardized format.
            :param ingest_result: The ingest result.
            return revised IngestResult with the crosswalked document in payload
            document during ingest
        """
        logger.info("CafeCrosswalk::transform()")
        logger.info("Transforming ingest result: %s", ingest_result)

        output_payload = IngestPayload(ingest_result.ingest_source_descriptor)
        payload_len = self.get_payload_length(ingest_result)
        logger.info(f"payload len: {payload_len}")
        for i in range(payload_len):
            payload = self.payload_resolve(ingest_result, i)
            logger.info(f"payload is resolved: {payload}")
            transformed = self.translate_to_accel_model(ingest_result, payload)
            self.report_individual(output_payload, ingest_result.ingest_source_descriptor.ingest_item_id, transformed)

        return output_payload

    @staticmethod
    def translate_to_accel_model(ingest_result: IngestPayload, payload: dict) -> dict:
        """
        :param ingest_result:
        :param payload: input dict
        :return: output dict
        """
        logger.info("CafeCrosswalk::translate_to_accel_model()")
        logger.info("Transforming ingest result: %s", ingest_result)

        data = payload.get('data', {})
        metadata_blocks = data.get('metadataBlocks', {})
        citation = metadata_blocks.get("citation", None)

        for item in citation['fields']:
            if item['typeName'] == 'author':
                if item['multiple']:
                    value_list = item.get('value', [])

                    author_name = value_list[0]["authorName"]["value"]
                    logger.info(f"author_name: {author_name}")


        # Submission Info
        submission = SubmissionInfoModel()
        submission.submitter_name = payload.get('metadataBlocks', None)
        #submission.submitter_name =

        # Program
        program = AccelProgramModel()
        program.code = 'CHORDS'
        program.name = 'CHORDS'

        # Project
        project = AccelProjectModel()


        # Resource
        resource = AccelIntermediateResourceModel()

        technical = TechnicalMetadataModel()
        technical.created = ingest_result.ingest_source_descriptor.submit_date

        rendered = build_accel_from_model(
            version="1.0.0",
            submission=submission,
            data_resource=None,
            temporal=None,
            population=None,
            geospatial=None,
            program=program,
            project=project,
            resource=resource,
            technical=technical
        )
        logger.info("Rendered model: %s", rendered)

        return rendered


