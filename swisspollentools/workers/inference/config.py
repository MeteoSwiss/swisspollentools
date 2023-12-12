"""
SwissPollenTools Inference Worker Configuration

The `config.py` module defines the configuration class for the Inference 
Worker in the SwissPollenTools library. It allows flexibility in specifying 
which data sources to use for inference, the input shape and precision, batch
size, and post-processing function.
"""
from typing import Optional, List, Dict, Union, Tuple, Callable
from dataclasses import dataclass, InitVar

@dataclass
class InferenceWorkerConfig():
    """
    Configuration class for the InferenceWorker, specifying parameters for
    performing inference on extracted data.

    Parameters:
    - `inw_from_rec0` (bool): Flag indicating whether to perform inference 
    using data from rec0, default is True.
    - `inw_from_rec1` (bool): Flag indicating whether to perform inference
    using data from rec1, default is True.
    - `inw_from_fluorescence` (bool): Flag indicating whether to perform
    inference using fluorescence data, default is True.
    - `inw_from_fluorescence_keys` (InitVar[Optional[Union[Dict, List]]]):
    Initial variable for specifying keys or indices to extract from the
    fluorescence data. If None, all fluorescence data is used. It can be a list
    of keys or adictionary mapping output names to keys, default is None.
    - `inw_rec_shape` (Tuple[int, int]): Tuple specifying the shape of the
    input recordings for inference, default is (200, 200).
    - `inw_rec_precision` (int): Integer specifying the bit precision for the
    input recordings, default is 16.
    - `inw_batch_size` (int): Integer specifying the batch size for inference,
    default is 1024.
    - `inw_post_processing_fn` (InitVar[Optional[Callable]]): Initial variable
    for specifying a post-processing function to be applied to the inference
    results. If None, a default identity function is used, default is None.
    """
    inw_from_rec0: bool=True
    inw_from_rec1: bool=True
    inw_from_fluorescence: bool=True
    inw_from_fluorescence_keys: InitVar[Optional[Union[Dict, List]]]=None
    inw_rec_shape: Tuple[int, int]=(200, 200)
    inw_rec_precision: int=16
    inw_batch_size: int=1024
    inw_post_processing_fn: InitVar[Optional[Callable]]=None

    def __post_init__(
        self,
        inw_from_fluorescence_keys,
        inw_post_processing_fn
    ):
        """
        Post-initialization method for the InferenceWorkerConfig class.

        Parameters:
        - `inw_from_fluorescence_keys` (InitVar[Optional[Union[Dict, List]]]):
        Initial variable for specifying keys or indices to extract from the
        fluorescence data. If None, all fluorescence data is used. It can be a
        list of keys or a dictionary mapping output names to keys, default is
        None.
        - `inw_post_processing_fn` (InitVar[Optional[Callable]]): Initial
        variable for specifying a post-processing function to be applied to the
        inference results. If None, a default identity function is used,
        default is None.
        """
        if not inw_from_fluorescence_keys:
            self.inw_from_fluorescence_keys = {}
        elif isinstance(inw_from_fluorescence_keys, list):
            self.inw_from_fluorescence_keys = \
                {el: el for el in inw_from_fluorescence_keys}
            
        if (self.inw_from_fluorescence) and (not self.inw_from_fluorescence_keys):
            self.inw_from_fluorescence = False

        if not inw_post_processing_fn:
            inw_post_processing_fn = lambda x: x
        self.inw_post_processing_fn = inw_post_processing_fn
