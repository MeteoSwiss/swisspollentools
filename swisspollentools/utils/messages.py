from typing import Callable, Dict

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

def assert_ismsg(func: Callable) -> bool:
    """
    Decorator to assert that the input is a valid message dictionary before
    calling a function.

    The decorator checks if the input dictionary has a request type using the
    `ismsg` function. If the input is not a valid message dictionary, a
    ValueError is raised.

    Parameters:
    - func (Callable): The function to be decorated.

    Returns:
    Callable: The decorated function.
    """
    def wrapper(msg: dict, *args, **kwargs):
        if not ismsg(msg,):
            raise ValueError(
                f"Calling `{func.__name__}` on non-message dictionnary"
            )
        
        return func(msg, *args, **kwargs)
    
    return wrapper

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

@assert_ismsg
def iseot(msg: dict) -> bool:
    """
    Check if the message represents an EndOfTask signal.

    Parameters:
    - msg (dict): The message dictionary to be checked.
    """
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

@assert_ismsg
def iseop(msg: dict) -> bool:
    """
    Check if the message represents an EndOfProcess signal.

    Parameters:
    - msg (dict): The message dictionary to be checked.

    Returns:
    bool: True if the message is an EndOfProcess message, False otherwise.
    """
    return msg[REQUEST_TYPE_KEY] == END_OF_PROCESS_VALUE

def ExpectedNItems(n_items: int):
    """
    Create a message for forwarding the number of items.

    Returns:
    dict: ExpectedNItems message.
    """
    return {
        REQUEST_TYPE_KEY: EXPECTED_N_ITEMS_VALUE,
        N_ITEMS_KEY: n_items
    }

def ExNIt(*args, **kwargs):
    """
    Alias function for ExpectedNItems.

    Returns:
    dict: ExpectedNItems message.
    """
    return ExpectedNItems(*args, **kwargs)

@assert_ismsg
def isexnit(msg: dict):
    """
    Check if the message is an ExpectedNItems message.

    Parameters:
    - msg (dict): The message dictionary to be checked.

    Returns:
    bool: True if the message is an ExpectedNItems message, False otherwise.
    """
    return msg[REQUEST_TYPE_KEY] == EXPECTED_N_ITEMS_VALUE

@assert_ismsg
def get_header(msg: Dict) -> Dict:
    """
    Return the header of a message.

    Parameters:
    - msg (dict): The message to extract the header from.

    Returns:
    dict: The header of the message passed as argument.
    """
    return get_subdictionary(msg, HEADER_KEY, sep=KEY_SEP)

@assert_ismsg
def get_body(msg: Dict) -> Dict:
    """
    Return the body of a message.

    Parameters:
    - msg (dict): The message to extract the body from.

    Returns:
    dict: The body of the message passed as argument.
    """
    return get_subdictionary(msg, BODY_KEY, sep=KEY_SEP)
