from swisspollentools.utils import \
    ATTRIBUTE_SEP, EXTRACTION_WORKER_PREFIX, \
    INFERENCE_WORKER_PREFIX, TOCSVW_WORKER_PREFIX, \
    MERGE_WORKER_PREFIX, get_subdictionary
from swisspollentools.workers import \
    ExtractionRequest, ZipExtraction, \
    InferenceRequest, Inference, \
    MergeRequest, Merge, \
    ToCSVRequest, ToCSV

def InferencePipeline(config, **kwargs):
    exw_config, inw_config, tocsvw_config = config
    exw_kwargs = get_subdictionary(
        kwargs,
        EXTRACTION_WORKER_PREFIX, ATTRIBUTE_SEP
    )
    inw_kwargs = get_subdictionary(
        kwargs,
        INFERENCE_WORKER_PREFIX, ATTRIBUTE_SEP
    )
    tocsvw_kwargs = get_subdictionary(
        kwargs,
        TOCSVW_WORKER_PREFIX, ATTRIBUTE_SEP
    )

    def run(file_path):
        out = ExtractionRequest(file_path=file_path)
        out = ZipExtraction(out, exw_config, **exw_kwargs)
        out = (InferenceRequest(file_path, batch_id, response=el) \
                    for batch_id, el in enumerate(out))
        out = (Inference(el, inw_config, **inw_kwargs).__next__() \
                    for el in out)
        out = (ToCSVRequest(file_path, batch_id, response=el) \
                    for batch_id, el in enumerate(out))
        out = (ToCSV(el, tocsvw_config, **tocsvw_kwargs).__next__() \
                    for el in out)

        return list(out)

    return run

def MergedInferencePipeline(config, **kwargs):
    exw_config, inw_config, mew_config, tocsvw_config = config
    exw_kwargs = get_subdictionary(
        kwargs,
        EXTRACTION_WORKER_PREFIX, ATTRIBUTE_SEP
    )
    inw_kwargs = get_subdictionary(
        kwargs,
        INFERENCE_WORKER_PREFIX, ATTRIBUTE_SEP
    )
    mew_kwargs = get_subdictionary(
        kwargs,
        MERGE_WORKER_PREFIX, ATTRIBUTE_SEP
    )
    tocsvw_kwargs = get_subdictionary(
        kwargs,
        TOCSVW_WORKER_PREFIX, ATTRIBUTE_SEP
    )

    def run(file_path):
        out = ExtractionRequest(file_path=file_path)
        out = ZipExtraction(out, exw_config, **exw_kwargs)
        out = (InferenceRequest(file_path, None, response=el) \
                    for batch_id, el in enumerate(out))
        out = (Inference(el, inw_config, **inw_kwargs).__next__() \
                    for el in out)
        out = (MergeRequest(file_path, None, el) \
                    for el in out)
        out = Merge(list(out), mew_config, **mew_kwargs)
        out = [ToCSVRequest(file_path, None, response=out)]
        out = (ToCSV(el, tocsvw_config, **tocsvw_kwargs).__next__() \
                    for el in out)

        return list(out)

    return run
