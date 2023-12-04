from typing import Any, Dict, List, Tuple

from swisspollentools.utils import *

def ToCSVRequest(
    file_path: str,
    batch_id: Any=None,
    response: Dict={}
):
    if not ismsg(response):
        raise ValueError()
    
    msg = {REQUEST_TYPE_KEY: TOCSV_REQUEST_VALUE}
    msg[FILE_PATH_KEY] = file_path
    msg[BATCH_ID_KEY] = batch_id
    msg[BODY_KEY] = get_body(response)

    msg = flatten_dictionary(msg)
    return msg

def ToCSVReq(*args, **kwargs) -> Dict:
    return ToCSVRequest(*args, **kwargs)

def istocsvreq(msg: Dict) -> bool:
    if not ismsg(msg):
        raise ValueError("Calling `istocsvreq` on non-message dictionnary")
    
    return msg[REQUEST_TYPE_KEY] == TOCSV_REQUEST_VALUE

def ToCSVResponse(
    file_path: str,
    batch_id: Any=None,
    *args,
    **kwargs
):
    msg = {REQUEST_TYPE_KEY: TOCSV_RESPONSE_VALUE}
    msg[FILE_PATH_KEY] = file_path
    msg[BATCH_ID_KEY] = batch_id

    msg = flatten_dictionary(msg)
    return msg

def ToCSVRes(*args, **kwargs):
    return ToCSVResponse(*args, **kwargs)

def istocsvres(msg: Dict) -> bool:
    if not ismsg(msg):
        raise ValueError("Calling `istocsvres` on non-message dictionnary")
    
    return msg[REQUEST_TYPE_KEY] == TOCSV_RESPONSE_VALUE
