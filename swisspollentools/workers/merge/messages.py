from typing import Any, Dict

from swisspollentools.utils import *

def MergeRequest(
    file_path: str,
    batch_id: Any=None,
    response: Dict={}
) -> Dict:
    if not ismsg(response):
        raise ValueError()

    msg = {REQUEST_TYPE_KEY: MERGE_REQUEST_VALUE}
    msg[FILE_PATH_KEY] = str(file_path)
    msg[BATCH_ID_KEY] = batch_id
    msg[BODY_KEY] = get_body(response)

    msg = flatten_dictionary(msg)
    return msg

def MeReq(*args, **kwargs) -> Dict:
    return MergeRequest(*args, **kwargs)

@assert_ismsg
def ismereq(msg: Dict) -> bool:    
    return msg[REQUEST_TYPE_KEY] == MERGE_REQUEST_VALUE

def MergeResponse(
    file_path: str,
    batch_id: Any=None,
    *args,
    **kwargs
) -> Dict:
    msg = {REQUEST_TYPE_KEY: MERGE_RESPONSE_VALUE}
    msg[FILE_PATH_KEY] = str(file_path)
    msg[BATCH_ID_KEY] = batch_id

    for k, v in enumerate(args):
        k = KEY_SEP.join([BODY_KEY, str(k)])
        msg[k] = v

    for k, v in kwargs.items():
        k = KEY_SEP.join([BODY_KEY, k])
        msg[k] = v

    msg = flatten_dictionary(msg)
    return msg

def MeRes(*args, **kwargs) -> Dict:
    return MergeResponse(*args, **kwargs)

@assert_ismsg
def ismeres(msg: Dict) -> bool:    
    return msg[REQUEST_TYPE_KEY] == MERGE_RESPONSE_VALUE
