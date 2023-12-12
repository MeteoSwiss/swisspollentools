from typing import Any, Dict, List, Tuple

from swisspollentools.utils import *

def InferenceRequest(
    file_path: str,
    batch_id: Any=None,
    response: Dict={},
):
    """
    Creates an Inference Request message.

    Parameters:
    - file_path (str): The file path for which inference is requested.
    - batch_id (Any, optional): The batch ID associated with the inference
    request.
    - response (Dict, optional): The response dictionary containing metadata,
      fluorescence data, and recordings. Default is an empty dictionary.

    Raises:
    - ValueError: If the provided response is not a valid message dictionary.

    Returns:
    dict: Inference Request message.
    """
    if not ismsg(response):
        raise ValueError()

    metadata = get_subdictionary(response, METADATA_KEY, KEY_SEP)
    fluorescence_data = get_subdictionary(response, FLUODATA_KEY, KEY_SEP)
    rec0 = response[REC0_KEY] if REC0_KEY in response.keys() else None
    rec1 = response[REC1_KEY] if REC1_KEY in response.keys() else None

    msg = {REQUEST_TYPE_KEY: INFERENCE_REQUEST_VALUE}
    msg[FILE_PATH_KEY] = str(file_path)
    msg[BATCH_ID_KEY] = batch_id

    if metadata is not None:
        msg[METADATA_KEY] = metadata

    if fluorescence_data is not None:
        msg[FLUODATA_KEY] = fluorescence_data

    if rec0 is not None:
        msg[REC0_KEY] = rec0

    if rec1 is not None:
        msg[REC1_KEY] = rec1

    msg = flatten_dictionary(msg)
    return msg

def InReq(*args, **kwargs):
    """
    Alias for InferenceRequest.
    """
    return InferenceRequest(*args, **kwargs)

@assert_ismsg
def isinreq(msg: Dict) -> bool:
    """
    Returns True if the message is an Inference Request message.

    Parameters:
    - msg (dict): Message dictionary.

    Raises:
    - ValueError: If called on a non-message dictionary.
    """    
    return msg[REQUEST_TYPE_KEY] == INFERENCE_REQUEST_VALUE

def parseinreq(msg: Dict) -> Tuple[Dict, Dict, List[int], List[int]]:
    """
    Parses an Inference Request message and returns metadata, fluorescence
    data, rec0, and rec1.

    Parameters:
    - msg (dict): Message dictionary.

    Raises:
    - ValueError: If called on a non-Inference Request dictionary.

    Returns:
    Tuple[Dict, Dict, List[int], List[int]]: Tuple containing metadata,
    fluorescence data, rec0, and rec1.
    """
    if not isinreq(msg):
        raise ValueError(
            "Calling `parseinreq` on non Inference Request dictionnary"
        )
    
    metadata = get_subdictionary(msg, METADATA_KEY, KEY_SEP)
    fluorescence_data = get_subdictionary(msg, FLUODATA_KEY, KEY_SEP)
    rec0 = msg[REC0_KEY] if REC0_KEY in msg.keys() else None
    rec1 = msg[REC1_KEY] if REC1_KEY in msg.keys() else None

    return metadata, fluorescence_data, rec0, rec1

def InferenceResponse(
    file_path: str,
    batch_id: Any=None,
    metadata=None,
    prediction=None,
    *args,
    **kwargs
) -> Dict:
    """
    Creates an Inference Response message.

    Parameters:
    - file_path (str): The file path associated with the inference response.
    - batch_id (Any, optional): The batch ID associated with the inference
    response.
    - metadata (Dict, optional): Metadata dictionary, default is None.
    - prediction (Any, optional): Prediction result, default is None.
    - *args: Additional positional arguments.
    - **kwargs: Additional keyword arguments.

    Returns:
    dict: Inference Response message.
    """
    msg = {REQUEST_TYPE_KEY: INFERENCE_RESPONSE_VALUE}
    msg[FILE_PATH_KEY] = str(file_path)
    msg[BATCH_ID_KEY] = batch_id

    if metadata is not None:
        msg[METADATA_KEY] = metadata

    if prediction is not None:
        msg[PREDICTION_KEY] = prediction

    for k, v in enumerate(args):
        k = KEY_SEP.join([BODY_KEY, str(k)])
        msg[k] = v

    for k, v in kwargs.items():
        k = KEY_SEP.join([BODY_KEY, k])
        msg[k] = v
    
    msg = flatten_dictionary(msg)
    return msg    

def InRep(*args, **kwargs):
    """
    Alias for InferenceResponse.
    """
    return InferenceResponse(*args, **kwargs)