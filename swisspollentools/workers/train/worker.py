import numpy as np
import tensorflow as tf
from sklearn.preprocessing import OneHotEncoder

import json

from typing import Dict, Generator

from swisspollentools.utils import *
from swisspollentools.workers.train.config import *
from swisspollentools.workers.train.messages import *

def Train(
    request: Dict,
    config: TrainWorkerConfig,
    **kwargs
) -> Generator:
    if not "model" in kwargs.keys():
        raise RuntimeError()
    if not "n_categories" in kwargs.keys():
        raise RuntimeError()
    if not "optimizer" in kwargs.keys():
        raise RuntimeError()
    if not "loss" in kwargs.keys():
        raise RuntimeError()

    ( fluorescence_data, rec0, rec1, 
        label ) = parsetrreq(request)

    try:
        fluorescence_data = {
            new_key: fluorescence_data[old_key] \
                for old_key, new_key in config.trw_from_fluorescence_keys.items()
        }
    except:
        print(fluorescence_data.keys())
        print(config.trw_from_fluorescence_keys)
    rec0 = rec0 / 2 ** config.trw_rec_precision
    rec1 = rec1 / 2 ** config.trw_rec_precision
    label = [[el] for el in label]

    encoder = OneHotEncoder()
    label = encoder.fit_transform(label).toarray()
    label = np.concatenate(
        [label, np.zeros((label.shape[0], kwargs["n_categories"] - label.shape[1]))],
        axis=1
    )

    dataset = ({
        "fluorescence_data": fluorescence_data if config.trw_from_fluorescence else None,
        "rec0": rec0 if config.trw_from_rec0 else None,
        "rec1": rec1 if config.trw_from_rec1 else None,
    }, label)
    dataset = tf.data.Dataset.from_tensor_slices(dataset)
    dataset = dataset.shuffle(len(label))
    dataset = dataset.batch(config.trw_batch_size, num_parallel_calls=tf.data.AUTOTUNE)
    dataset = dataset.cache().prefetch(buffer_size=tf.data.AUTOTUNE)

    kwargs["model"].compile(
        optimizer=kwargs["optimizer"],
        loss=kwargs["loss"],
    )
    kwargs["model"].fit(
        x=dataset,
        epochs=config.trw_n_epochs,
    )

    model_description = {
        "categories": encoder.categories_[0].tolist(),
        "n_categories": kwargs["n_categories"]
    }
    kwargs["model"].save_weights("model.weights")
    with open(config.trw_output_directory.joinpath("model.config"), "w") as f:
        json.dump(model_description, f)

    yield TrainResponse(
        file_path=config.trw_output_directory
    )

@PlPsCWorker
def TrainWorker(
    request: Dict,
    config,
    **kwargs
):
    if not istrreq(request):
        raise ValueError()

    yield from TrainWorker(request, config, **kwargs)
