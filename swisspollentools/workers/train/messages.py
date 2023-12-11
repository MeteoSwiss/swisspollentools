from typing import Any, Dict, List, Tuple

from swisspollentools.utils import *

def TrainRequest(
    file_path: str,
    batch_id: Any=None,
    response: Dict={}
) -> Dict:
    if not ismsg(response):
        raise ValueError()

    fluorescence_data = get_subdictionary(response, FLUODATA_KEY, KEY_SEP)
    rec0 = response[REC0_KEY] if REC0_KEY in response.keys() else None
    rec1 = response[REC1_KEY] if REC1_KEY in response.keys() else None
    label = response[LABEL_KEY] if LABEL_KEY in response.keys() else None

    msg = {REQUEST_TYPE_KEY: TRAIN_REQUEST_VALUE}
    msg[FILE_PATH_KEY] = str(file_path)
    msg[BATCH_ID_KEY] = batch_id

    if fluorescence_data is not None:
        msg[FLUODATA_KEY] = fluorescence_data
    
    if rec0 is not None:
        msg[REC0_KEY] = rec0
    
    if rec1 is not None:
        msg[REC1_KEY] = rec1
    
    if label is not None:
        msg[LABEL_KEY] = label

    msg = flatten_dictionary(msg)
    return msg

def TrReq(*args, **kwargs):
    return TrainRequest(*args, **kwargs)

@assert_ismsg
def istrreq(msg: Dict) -> bool:    
    return msg[REQUEST_TYPE_KEY] == TRAIN_REQUEST_VALUE

def parsetrreq(msg: Dict) -> Tuple[Dict, np.ndarray, np.ndarray, List[str]]:
    if not istrreq(msg):
        raise ValueError(
            "Calling `parsetrreq` on non Train Request dictionnary"
        )
    
    fluorescence_data = get_subdictionary(msg, FLUODATA_KEY, KEY_SEP)
    rec0 = msg[REC0_KEY] if REC0_KEY in msg.keys() else None
    rec1 = msg[REC1_KEY] if REC1_KEY in msg.keys() else None
    label = msg[LABEL_KEY] if LABEL_KEY in msg.keys() else None

    return fluorescence_data, rec0, rec1, label

def TrainResponse(
    file_path: str,
    batch_id: Any=None,
    *args,
    **kwargs
) -> Dict:
    msg = {REQUEST_TYPE_KEY: INFERENCE_RESPONSE_VALUE}
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

def TrRep(*args, **kwargs):
    return TrainResponse(*args, **kwargs)
