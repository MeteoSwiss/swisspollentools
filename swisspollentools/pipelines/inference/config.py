from swisspollentools.utils import *
from swisspollentools.workers import ExtractionWorkerConfig, InferenceWorkerConfig, \
    ToCSVWorkerConfig

def InferencePipelineConfig(**kwargs):
    exw_config = ExtractionWorkerConfig(
        **get_subdictionary(kwargs, EXTRACTION_WORKER_PREFIX, ATTRIBUTE_SEP, False)
    )
    inw_config = InferenceWorkerConfig(**{
        **get_subdictionary(kwargs, INFERENCE_WORKER_PREFIX, ATTRIBUTE_SEP, False)
    })
    tocsvw_config = ToCSVWorkerConfig(**{
        **get_subdictionary(kwargs, TOCSVW_WORKER_PREFIX, ATTRIBUTE_SEP, False)
    })

    return exw_config, inw_config, tocsvw_config
