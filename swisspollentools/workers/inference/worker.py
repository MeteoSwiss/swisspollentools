"""
Inference Worker: worker.py

This module contains functions for performing inference using a pre-trained
model based on the provided Inference Request messages. It also includes a
Pull-Push worker function to handle inference requests.

Functions:
- Inference: Perform inference on the provided data using a pre-trained model.
- InferenceWorker: Pull-Push-Control worker function for handling inference
requests.
"""
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
    """
    Perform inference on the provided data using a pre-trained model.

    Parameters:
    - request (Dict): Inference Request message containing metadata,
    fluorescence data, and recordings.
    - config (InferenceWorkerConfig): Configuration object specifying
    inference parameters.
    - **kwargs: Additional keyword arguments. It must contain a "model" key,
    pointing to the pre-trained model.

    Yields:
    InferenceResponse: A generator yielding Inference Response messages for
    each batch of predictions.

    Raises:
    - RuntimeError: If the "model" key is not present in the keyword arguments.

    Example:
    request_msg = InReq(
        file_path="./data/example.zip", batch_id=0, response=response_dict
    )
    inference_config = InferenceWorkerConfig(
        inw_from_rec0=True,
        inw_from_rec1=True,
        inw_from_fluorescence=True,
        inw_from_fluorescence_keys={"channel_1": "ch1", "channel_2": "ch2"},
        inw_rec_shape=(200, 200),
        inw_rec_precision=16,
        inw_batch_size=1024,
        inw_post_processing_fn=my_post_processing_function
    )
    model = load_pretrained_model()
    for inference_response in Inference(
        request_msg, inference_config, model=model
    ):
        # Process each Inference Response message
        pass
    """
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
    dataset = dataset.map(config.inw_pre_processing_fn)
    dataset = dataset.batch(config.inw_batch_size, num_parallel_calls=tf.data.AUTOTUNE)
    dataset = dataset.prefetch(buffer_size=tf.data.AUTOTUNE)

    predictions = kwargs["model"].predict(dataset, verbose=0)
    predictions = config.inw_post_processing_fn(predictions)

    yield InferenceResponse(
        file_path=request[FILE_PATH_KEY],
        batch_id=request[BATCH_ID_KEY],
        metadata=metadata,
        prediction=predictions
    )

@PullPushWorker
def InferenceWorker(
    request: Dict,
    config: InferenceWorkerConfig,
    **kwargs
) -> Generator:
    """
    Pull-Push-Control worker function for handling inference requests.

    Parameters:
    - request (dict): Inference Request message dictionary.
    - config (InferenceWorkerConfig): Configuration object.
    - **kwargs: Additional keyword arguments.

    Yields:
    InferenceResponse: A generator yielding Inference Response messages.

    Raises:
    - ValueError: If the provided message is not an Inference Request message.
    """
    if not isinreq(request):
        raise ValueError()
    
    yield from Inference(request, config, **kwargs)
