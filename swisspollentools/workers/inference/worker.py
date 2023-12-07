import numpy as np
import tensorflow as tf

from typing import Dict, Generator

from swisspollentools.utils import *
from swisspollentools.workers.inference.config import *
from swisspollentools.workers.inference.messages import *

def Inference(
    request: Dict,
    config: InferenceWorkerConfig,
    **kwargs
) -> Generator:
    if not "model" in kwargs.keys():
        raise RuntimeError()

    ( metadata, fluorescence_data,
        rec0, rec1 ) = parseinreq(request)

    fluorescence_data = {
        new_key: fluorescence_data[old_key] \
            for old_key, new_key in config.inw_from_fluorescence_keys.items()
    }
    rec0 = rec0 / 2 ** config.inw_rec_precision
    rec1 = rec1 / 2 ** config.inw_rec_precision

    dataset = {
        "fluorescence_data": fluorescence_data if config.inw_from_fluorescence else None,
        "rec0": rec0 if config.inw_from_rec0 else None,
        "rec1": rec1 if config.inw_from_rec1 else None
    }
    dataset = prune_dictionary(dataset)
    dataset = tf.data.Dataset.from_tensor_slices(dataset)
    dataset = dataset.batch(config.inw_batch_size, num_parallel_calls=tf.data.AUTOTUNE)
    dataset = dataset.prefetch(buffer_size=tf.data.AUTOTUNE)

    predictions = []
    for batch in dataset:
        prediction = kwargs["model"].predict(batch, verbose=False)
        prediction = config.inw_post_processing_fn(prediction)
        predictions.append(prediction)
    predictions = collate_fn(predictions, numpy_strategy="concatenate")

    yield InferenceResponse(
        file_path=request[FILE_PATH_KEY],
        batch_id=request[BATCH_ID_KEY],
        metadata=metadata,
        prediction=predictions
    )

@PlPsCWorker
def InferenceWorker(
    request: Dict,
    config: InferenceWorkerConfig,
    **kwargs
) -> Generator:
    if not isinreq(request):
        raise ValueError()
    
    yield from Inference(request, config, **kwargs)
