from typing import Any, Dict, Sequence
from collections.abc import MutableMapping
from itertools import islice

import numpy as np
import tensorflow as tf

def flatten(iterable):
    if isinstance(iterable, (list, tuple, set, range)):
        for sub in iterable:
            yield from flatten(sub)
    else:
        yield iterable

def flatten_dictionary(dictionary, parent_key='', separator='/'):
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(
                flatten_dictionary(value, new_key, separator=separator).items()
            )
        else:
            items.append((new_key, value))
    return dict(items)

def empty_structure(el):    
    if isinstance(el, tuple):
        return tuple(empty_structure(e) for e in el)
    
    if isinstance(el, dict):
        return {k: empty_structure(v) for k, v in el.items()}
    
    return []

def append_to_structure(el, structure):
    if isinstance(structure, tuple):
        return tuple(append_to_structure(e, s) for e, s in zip(el, structure))
    
    if isinstance(structure, dict):
        return {k: append_to_structure(el[k], v) for k, v in structure.items()}
    
    if not isinstance(structure, list):
        raise NotImplementedError()

    structure.append(el)
    return structure

def flatten_structure(structure):
    if isinstance(structure, tuple):
        return tuple(flatten_structure(s) for s in structure)
    
    if isinstance(structure, dict):
        return {k: flatten_structure(v) for k, v in structure.items()}
    
    if not isinstance(structure, list):
        raise NotImplementedError()

    if len(structure) == 0:
        raise ValueError()

    if isinstance(structure[0], list):
        return list(flatten(structure))

    if isinstance(structure[0], np.ndarray):
        return np.concatenate(structure, axis=0)
    
    if isinstance(structure[0], tf.Tensor):
        return tf.concat(structure, axis=0).numpy()

    return structure

def collate_fn(batch):
    collated_batch = empty_structure(batch[0])
    for el in batch:
        collated_batch = append_to_structure(el, collated_batch)
    collated_batch = flatten_structure(collated_batch)
    return collated_batch

def batchify(iterable, batch_size, callback=None, collate_fn=collate_fn):
    it = iter(iterable)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            return

        if callback is not None:
            yield callback(*collate_fn(batch))
        else:
            yield collate_fn(batch)

def isempty(value: Any):
    if value is None:
        return True
    
    if isinstance(value, Sequence):
        return len(value) == 0
    
    return False

def prune_dictionary(dictionary: Dict) -> Dict:
        return {k: v for k, v in dictionary.items() if not isempty(v)}

def get_subdictionary(dictionary: Dict, prefix: str, sep: str, remove: bool=True):
    if not prefix[-1] == sep:
        prefix = prefix + sep
    
    if remove:
        return {k.removeprefix(prefix): v \
                    for k, v in dictionary.items() \
                    if k.startswith(prefix)}

    return {k: v for k, v in dictionary.items() \
                if k.startswith(prefix)}
