from typing import Dict

from swisspollentools.utils.constants import *
from swisspollentools.utils.utils import *

def ismsg(msg: dict) -> bool:
    """
    Check if the dictionary represents a valid message by verifying the presence of a request type.

    Parameters:
    - msg (dict): The dictionary to be checked.

    Returns:
    bool: True if the message has a request type, False otherwise.

    Raises:
    - ValueError: If called on a non-dictionary object.
    """
    if not isinstance(msg, dict):
        raise ValueError("Calling `ismsg` on non-dictionnary object.")

    return REQUEST_TYPE_KEY in msg.keys()

def EndOfTask() -> dict:
    """
    Create a message signaling the completion of a task.

    Returns:
    dict: EndOfTask message.
    """
    return {REQUEST_TYPE_KEY: END_OF_TASK_VALUE}

def EOT() -> dict:
    """
    Alias function for EndOfTask.

    Returns:
    dict: EndOfTask message.
    """
    return EndOfTask()

def iseot(msg: dict) -> bool:
    """
    Check if the message represents an EndOfTask signal.

    Parameters:
    - msg (dict): The message dictionary to be checked.

    Returns:
    bool: True if the message is an EndOfTask message, False otherwise.

    Raises:
    - ValueError: If called on a non-message dictionary.
    """
    if not ismsg(msg):
        raise ValueError("Calling `iseot` on non-message dictionnary")

    return msg[REQUEST_TYPE_KEY] == END_OF_TASK_VALUE

def EndOfProcess() -> dict:
    """
    Create a message signaling the completion of a process.

    Returns:
    dict: EndOfProcess message.
    """
    return {REQUEST_TYPE_KEY: END_OF_PROCESS_VALUE}

def EOP() -> dict:
    """
    Alias function for EndOfProcess.

    Returns:
    dict: EndOfProcess message.
    """
    return EndOfProcess()

def iseop(msg: dict) -> bool:
    """
    Check if the message represents an EndOfProcess signal.

    Parameters:
    - msg (dict): The message dictionary to be checked.

    Returns:
    bool: True if the message is an EndOfProcess message, False otherwise.

    Raises:
    - ValueError: If called on a non-message dictionary.
    """
    if not ismsg(msg):
        raise ValueError("Calling `iseop` on non-message dictionnary")

    return msg[REQUEST_TYPE_KEY] == END_OF_PROCESS_VALUE

def ExpectedNItems(n_items: int):
    return {
        REQUEST_TYPE_KEY: EXPECTED_N_ITEMS_VALUE,
        N_ITEMS_KEY: n_items
    }

def ExNIt(*args, **kwargs):
    return ExpectedNItems(*args, **kwargs)

def isexnit(msg: dict):
    if not ismsg(msg):
        raise ValueError("Calling `isexnit` on non-message dictionnary")

    return msg[REQUEST_TYPE_KEY] == EXPECTED_N_ITEMS_VALUE

def get_header(msg: Dict) -> Dict:
    if not isinstance(msg, dict):
        raise ValueError("Calling `get_header` on non-dictionnary object.")

    return get_subdictionary(msg, HEADER_KEY, sep=KEY_SEP)

def get_body(msg: Dict) -> Dict:
    if not isinstance(msg, dict):
        raise ValueError("Calling `get_body` on non-dictionnary object.")

    return get_subdictionary(msg, BODY_KEY, sep=KEY_SEP)
