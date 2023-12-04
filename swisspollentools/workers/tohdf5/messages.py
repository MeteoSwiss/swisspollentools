from typing import Any, Dict, List, Tuple

from swisspollentools.utils import *

def ToHDF5Request(
    file_path: str,
    batch_id: Any=None,
    response: Dict={}
) -> Dict:
    if not ismsg(response):
        raise ValueError()
    
    msg = {REQUEST_TYPE_KEY: TOHDF5_REQUEST_VALUE}
    msg[FILE_PATH_KEY] = file_path
    msg[BATCH_ID_KEY] = batch_id
    msg[BODY_KEY] = get_body(response)

    msg = flatten_dictionary(msg)
    return msg

def ToHDF5Req(*args, **kwargs) -> Dict:
    return ToHDF5Request(*args, **kwargs)

def istohdf5req(msg: Dict) -> bool:
    if not ismsg(msg):
        raise ValueError("Calling `istocsvreq` on non-message dictionnary")
    
    return msg[REQUEST_TYPE_KEY] == TOHDF5_REQUEST_VALUE

def ToHDF5Response(
    file_path: str,
    batch_id: Any=None,
    *args,
    **kwargs
) -> Dict:
    msg = {REQUEST_TYPE_KEY: TOHDF5_RESPONSE_VALUE}
    msg[FILE_PATH_KEY] = file_path

    msg = flatten_dictionary(msg)
    return msg

def ToHDF5Res(*args, **kwargs):
    return ToHDF5Response(*args, **kwargs)
