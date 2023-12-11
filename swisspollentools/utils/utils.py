"""
Utility functions for the SwissPollenTools library.

This module provides various utility functions used in the SwissPollenTools
library.

Functions:
- flatten(iterable) -> generator: Flatten a nested iterable.
- flatten_dictionary(dictionary, parent_key='', separator='/') -> Dict: Flatten
a nested dictionary.
- empty_structure(el) -> Any: Create an empty structure with the same shape as
the input.
- append_to_structure(el, structure) -> Any: Append an element to a structure.
- flatten_structure(structure, list_strategy=None, numpy_strategy="stack") -> 
Any: Flatten a nested structure with optional strategies for lists and NumPy
arrays.
- collate_fn(batch, *args, **kwargs) -> Any: Collate a batch of structured 
data.
- batchify(iterable, batch_size, callback=None, collate_fn=collate_fn, *args,
**kwargs) -> generator: Batchify an iterable, applying a collate function.
- isempty(value: Any) -> bool: Check if a value is empty.
- prune_dictionary(dictionary: Dict) -> Dict: Remove entries with empty values
from a dictionary.
- get_subdictionary(dictionary: Dict, prefix: str, sep: str, remove: bool=True)
-> Dict: Get a subdictionary with keys starting with a specified prefix.
"""
from typing import Any, Callable, Dict, Generator, Iterable, List, \
    Optional, Sequence, Tuple, Union
from collections.abc import MutableMapping
from itertools import islice

import numpy as np
import tensorflow as tf

def flatten(iterable: Any) -> Generator:
    """Flatten a nested iterable."""
    if isinstance(iterable, (list, tuple, set, range)):
        for sub in iterable:
            yield from flatten(sub)
    else:
        yield iterable

def flatten_dictionary(
    dictionary: Dict,
    parent_key: str='',
    separator: str='/'
) -> Dict:
    """Flatten a nested dictionary."""
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

def empty_structure(el: Any) -> Union[Dict, List, Tuple]:
    """Create an empty structure with the same shape as the input."""    
    if isinstance(el, tuple):
        return tuple(empty_structure(e) for e in el)
    
    if isinstance(el, dict):
        return {k: empty_structure(v) for k, v in el.items()}
    
    return []

def append_to_structure(
    el: Any,
    structure: Union[Dict, List, Tuple]
) -> Union[Dict, List, Tuple]:
    """Append an element to a structure."""
    if isinstance(structure, tuple):
        return tuple(append_to_structure(e, s) for e, s in zip(el, structure))
    
    if isinstance(structure, dict):
        return {k: append_to_structure(el[k], v) for k, v in structure.items()}
    
    if not isinstance(structure, list):
        raise NotImplementedError()

    structure.append(el)
    return structure

def flatten_structure(
    structure: Union[Dict, List, Tuple],
    list_strategy: Optional[str]=None,
    numpy_strategy: str="stack"
) -> Union[Dict, List, Tuple]:
    """
    Flatten a nested structure with optional strategies for lists and NumPy
    arrays.
    
    Parameters:
    - structure (Any): The input structured data.
    - list_strategy (str, optional): Strategy for flattening list of lists
    ("concatenate" or None).
    - numpy_strategy (str, optional): Strategy for flattening list of NumPy 
    arrays or Tensorflow tensors ("concatenate" or "stack").

    Returns:
    Any: Flattened structure.
    """
    if isinstance(structure, tuple):
        return tuple(flatten_structure(s, list_strategy, numpy_strategy) \
                        for s in structure)
    
    if isinstance(structure, dict):
        return {k: flatten_structure(v, list_strategy, numpy_strategy) \
                        for k, v in structure.items()}
    
    if not isinstance(structure, list):
        raise NotImplementedError()

    if len(structure) == 0:
        raise ValueError()

    if isinstance(structure[0], list):
        if list_strategy is None:
            return structure
        elif list_strategy == "concatenate":
            return [el for s in structure for el in s]
        else:
            raise NotImplementedError()

    if isinstance(structure[0], np.ndarray):
        if numpy_strategy == "concatenate":
            return np.concatenate(structure, axis=0)
        elif numpy_strategy == "stack":
            return np.stack(structure)
        else:
            raise NotImplementedError()
    
    if isinstance(structure[0], tf.Tensor):
        if numpy_strategy == "concatenate":
            return tf.concat(structure, axis=0).numpy()
        elif numpy_strategy == "stack":
            return tf.stack(structure).numpy()
        else:
            raise NotImplementedError()

    return structure

def collate_fn(
    batch: List,
    *args,
    **kwargs
) -> Union[Dict, List, Tuple]:
    """
    Collate a batch of structured data.

    Parameters:
    - batch (list): List of structured data.
    - *args (Any): Additional arguments.
    - **kwargs (Any): Additional keyword arguments.

    Returns:
    Any: Collated batch.

    Example:
    # Example structured data
    structured_data = [
        {'a': 1, 'b': [2, 3], 'c': {'d': 4}},
        {'a': 5, 'b': [6, 7], 'c': {'d': 8}},
        {'a': 9, 'b': [10, 11], 'c': {'d': 12}}
    ]

    # Apply the collate function to the structured data
    collated_data = collate_fn(
        structured_data, list_strategy='concatenate'
    )

    # Output
    # {
        'a':[1, 5, 9],
        'b': [[2, 3], [6, 7], [10, 11]],
        'c': {'d': [4, 8, 12]}
    }
    """
    collated_batch = empty_structure(batch[0])
    for el in batch:
        collated_batch = append_to_structure(el, collated_batch)
    collated_batch = flatten_structure(collated_batch, *args, **kwargs)
    return collated_batch

def batchify(
    iterable: Iterable,
    batch_size: int,
    callback: Optional[Callable]=None,
    collate_fn: Callable=collate_fn,
    *args,
    **kwargs
) -> Generator:
    """
    Batchify an iterable, applying a collate function.

    Parameters:
    - iterable (iterable): Input iterable.
    - batch_size (int): Size of each batch.
    - callback (function, optional): Callback function to apply to each batch.
    - collate_fn (function, optional): Collate function to apply to each batch.
    - *args (Any): Additional arguments for collate function.
    - **kwargs (Any): Additional keyword arguments for collate function.

    Returns:
    generator: Batched iterable.

    Example:
    # Example data
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    # Define a simple collate function that sums the batch
    def sum_collate(batch):
        return sum(batch)

    # Batchify the data with a batch size of 3 and apply the collate
    # function
    batched_data = list(batchify(data, batch_size=3, collate_fn=sum_collate))

    # Output
    # [6, 15, 24, 10]

    Note:
    - The batchify function creates batches of the specified size from the
    input iterable and applies a collate function to each batch for further
    processing.
    """
    it = iter(iterable)
    while True:
        batch = list(islice(it, batch_size))
        if not batch:
            return

        if callback is not None:
            yield callback(*collate_fn(batch, *args, **kwargs))
        else:
            yield collate_fn(batch, *args, **kwargs)

def isempty(value: Any) -> bool:
    """
    Check if a value is empty.

    Parameters:
    - value (Any): The input value.

    Returns:
    bool: True if the value is empty, False otherwise.
    """
    if value is None:
        return True
    
    if isinstance(value, Sequence):
        return len(value) == 0
    
    return False

def prune_dictionary(dictionary: Dict) -> Dict:
    """
    Remove entries with empty values from a dictionary.

    Parameters:
    - dictionary (Dict): Input dictionary.

    Returns:
    Dict: Dictionary with empty entries removed.
    """
    return {k: v for k, v in dictionary.items() if not isempty(v)}

def get_subdictionary(
    dictionary: Dict,
    prefix: str,
    sep: str,
    remove: bool=True
):
    """
    Get a subdictionary with keys starting with a specified prefix.

    This function retrieves a subdictionary from the input dictionary, 
    including only the key-value pairs whose keys start with the specified
    prefix. The prefix can be removed from the keys in the subdictionary.

    Parameters:
    - dictionary (Dict): The input dictionary.
    - prefix (str): The prefix used to filter keys.
    - sep (str): The separator used in keys.
    - remove (bool, optional): Whether to remove the prefix from the keys 
    (default is True).

    Returns:
    Dict: The subdictionary containing filtered key-value pairs.

    Example:
    # Example data
    data = {
        'prefix_key1': 42, 'prefix_key2': 'value2', 'other_key': 'value3'
    }

    # Output
    # {'key1': 42, 'key2': 'value2'}

    Note:
    - If `remove` is set to True, the prefix is removed from the keys in the
    subdictionary.
    - If `remove` is set to False, the keys in the subdictionary include the
    full original keys.
    """
    if not prefix[-1] == sep:
        prefix = prefix + sep
    
    if remove:
        return {k.removeprefix(prefix): v \
                    for k, v in dictionary.items() \
                    if k.startswith(prefix)}

    return {k: v for k, v in dictionary.items() \
                if k.startswith(prefix)}
