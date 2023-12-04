from typing import Optional, List, Dict, Union, Tuple, Callable
from dataclasses import dataclass, InitVar

__all__ = ["InferenceWorkerConfig"]

@dataclass
class InferenceWorkerConfig():
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
