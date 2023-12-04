from  multiprocessing import Process

from swisspollentools.scaffolds import Collator, Sink, Ventilator
from swisspollentools.utils import *
from swisspollentools.workers import ExtractionRequest, ExtractionWorker, \
    InferenceRequest, InferenceWorker, ToCSVRequest, ToCSVWorker

def HPCInferencePipeline(
    config,
    n_exw,
    n_inw,
    n_tocsvw,
    ports,
    c_ports,
    s_ports,
    **kwargs
):
    exw_config, inw_config, tocsvw_config = config

    __v_kwargs = get_subdictionary(kwargs, "__v", ATTRIBUTE_SEP)
    __c1_kwargs = get_subdictionary(kwargs, "__c1", ATTRIBUTE_SEP)
    __c2_kwargs = get_subdictionary(kwargs, "__c2", ATTRIBUTE_SEP)
    __s_kwargs = get_subdictionary(kwargs, "__s", ATTRIBUTE_SEP)

    exw_kwargs = get_subdictionary(kwargs, EXTRACTION_WORKER_PREFIX, ATTRIBUTE_SEP)
    inw_kwargs = get_subdictionary(kwargs, INFERENCE_WORKER_PREFIX, ATTRIBUTE_SEP)
    tocsvw_kwargs = get_subdictionary(kwargs, TOCSVW_WORKER_PREFIX, ATTRIBUTE_SEP)

    def run(sequence):
        ventilator = Process(
            target=Ventilator,
            args=(sequence, ExtractionRequest, ports[0], s_ports[0]),
            kwargs=__v_kwargs
        )
        collator_1 = Process(
            target=Collator,
            args=(InferenceRequest, ports[1], ports[2], c_ports[0], (s_ports[0], s_ports[1])),
            kwargs=__c1_kwargs
        )
        collator_2 = Process(
            target=Collator,
            args=(ToCSVRequest, ports[3], ports[4], c_ports[1], (s_ports[1], s_ports[2])),
            kwargs=__c2_kwargs
        )
        sink = Process(
            target=Sink,
            args=(ports[5], c_ports[2], s_ports[2]),
            kwargs=__s_kwargs
        )

        ventilator.start()
        collator_1.start()
        collator_2.start()
        sink.start()

        extraction_workers = [Process(
            target=ExtractionWorker,
            args=(exw_config, ports[0], ports[1], c_ports[0]),
            kwargs=exw_kwargs
        ) for _ in range(n_exw)]
        inference_workers = [Process(
            target=InferenceWorker,
            args=(inw_config, ports[2], ports[3], c_ports[1]),
            kwargs=inw_kwargs
        ) for _ in range(n_inw)]
        tocsv_workers = [Process(
            target=ToCSVWorker,
            args=(tocsvw_config, ports[4], ports[5], c_ports[2]),
            kwargs=tocsvw_kwargs
        ) for _ in range(n_tocsvw)]

        for worker in extraction_workers:
            worker.start()
        for worker in inference_workers:
            worker.start()
        for worker in tocsv_workers:
            worker.start()

        return

    return run