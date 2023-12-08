from swisspollentools.utils import \
    ATTRIBUTE_SEP, EXTRACTION_WORKER_PREFIX, \
    INFERENCE_WORKER_PREFIX, TOCSVW_WORKER_PREFIX, \
    MERGE_WORKER_PREFIX, get_subdictionary
from swisspollentools.workers import \
    ExtractionWorkerConfig, InferenceWorkerConfig, \
    MergeWorkerConfig, ToCSVWorkerConfig

def InferencePipelineConfig(**kwargs):
    exw_config = ExtractionWorkerConfig(
        **get_subdictionary(
            kwargs,
            EXTRACTION_WORKER_PREFIX, ATTRIBUTE_SEP, False
        )
    )
    inw_config = InferenceWorkerConfig(**{
        **get_subdictionary(
            kwargs,
            INFERENCE_WORKER_PREFIX, ATTRIBUTE_SEP, False
        )
    })
    tocsvw_config = ToCSVWorkerConfig(**{
        **get_subdictionary(
            kwargs,
            TOCSVW_WORKER_PREFIX, ATTRIBUTE_SEP, False
        )
    })

    return exw_config, inw_config, tocsvw_config

def MergedInferencePipelineConfig(**kwargs):
    exw_config = ExtractionWorkerConfig(
        **get_subdictionary(
            kwargs,
            EXTRACTION_WORKER_PREFIX, ATTRIBUTE_SEP, False
        )
    )
    inw_config = InferenceWorkerConfig(**{
        **get_subdictionary(
            kwargs,
            INFERENCE_WORKER_PREFIX, ATTRIBUTE_SEP, False
        )
    })
    mew_config = MergeWorkerConfig(**{
        **get_subdictionary(
            kwargs,
            MERGE_WORKER_PREFIX, ATTRIBUTE_SEP, False
        )
    })
    tocsvw_config = ToCSVWorkerConfig(**{
        **get_subdictionary(
            kwargs,
            TOCSVW_WORKER_PREFIX, ATTRIBUTE_SEP, False
        )
    })

    return exw_config, inw_config, mew_config, tocsvw_config
