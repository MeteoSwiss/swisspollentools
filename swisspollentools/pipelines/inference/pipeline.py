from swisspollentools.utils import *
from swisspollentools.workers import ExtractionRequest, ZipExtraction, \
    InferenceRequest, Inference, ToCSVRequest, ToCSV

def InferencePipeline(config, **kwargs):
    exw_config, inw_config, tocsvw_config = config
    exw_kwargs = get_subdictionary(kwargs, EXTRACTION_WORKER_PREFIX, ATTRIBUTE_SEP)
    inw_kwargs = get_subdictionary(kwargs, INFERENCE_WORKER_PREFIX, ATTRIBUTE_SEP)
    tocsvw_kwargs = get_subdictionary(kwargs, TOHDF5_WORKER_PREFIX, ATTRIBUTE_SEP)

    def run(file_path):
        out = ExtractionRequest(file_path=file_path)
        out = ZipExtraction(out, exw_config, **exw_kwargs)
        out = (InferenceRequest(file_path, batch_id, response=el) for batch_id, el in enumerate(out))
        out = (Inference(el, inw_config, **inw_kwargs).__next__() for el in out)
        out = (ToCSVRequest(file_path, batch_id, response=el) for batch_id, el in enumerate(out))
        out = (ToCSV(el, tocsvw_config, **tocsvw_kwargs).__next__() for el in out)

        return [el for el in out]

    return run