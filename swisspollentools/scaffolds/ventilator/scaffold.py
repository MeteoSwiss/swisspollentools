"""
Ventilator Scaffold for Starting a Pipeline by Sending Requests from an 
Iterable.

The Ventilator scaffold is used to start a pipeline by sending multiple 
requests built on top of an iterable in an HPC (High-Performance Computing)
environment. It utilizes PyZMQ for communication and leverages utility
functions from the SwissPollenTools library.
"""
import time
from typing import Iterable, Callable, Optional

import zmq

from swisspollentools.utils import \
    LAUNCH_SLEEP_TIME, \
    send_request, ExpectedNItems

def Ventilator(
    iterable: Iterable,
    request_fn: Callable,
    push_port: int,
    scaffold_port: int,
    on_startup: Optional[Callable]=None,
    on_closure: Optional[Callable]=None,
    **kwargs
):
    """
    Create and run a Ventilator scaffold for starting a pipeline and sending
    requests.

    Args:
    - iterable (Iterable): An iterable containing elements to generate
    requests.
    - request_fn (Callable): A function that takes an element from the iterable
    and returns a request.
    - push_port (int): The port for sending requests to the first layer of
    workers.
    - scaffold_port (int): The port for scaffold communication.
    - on_startup (Optional[Callable]): An optional callback function to execute
    on scaffold startup.
    - on_closure (Optional[Callable]): An optional callback function to execute
    on scaffold closure.
    - **kwargs (Any): Additional keyword arguments.

    Returns:
    None

    Example:
    # Example usage of the Ventilator scaffold
    from swisspollentools.ventilator import Ventilator

    # Define an iterable (e.g., list of elements)
    elements = [1, 2, 3, 4, 5]

    # Define a request function that transforms an element into a request
    def request_fn(element):
        return {"data": element}

    Ventilator(
        iterable=elements,
        request_fn=request_fn,
        push_port=5555,
        scaffold_port=5556,
        on_startup=lambda: print("Ventilator scaffold started"),
        on_closure=lambda: print("Ventilator scaffold closed")
    )

    Note:
    - The Ventilator scaffold creates ZeroMQ sockets for communication, sends
    requests generated from the iterable to the first layer of workers, and
    signals the end of the process when the expected number of tasks is sent.
    """
    if on_startup is not None:
        on_startup()

    context = zmq.Context()

    # Set PUSH binding
    sender = context.socket(zmq.PUSH)
    sender.bind(f"tcp://127.0.0.1:{push_port}")

    scaffold_sender = context.socket(zmq.PAIR)
    scaffold_sender.bind(f"tcp://127.0.0.1:{scaffold_port}")

    time.sleep(LAUNCH_SLEEP_TIME)

    n_tasks_counter = 0
    for el in iterable:
        send_request(sender, request_fn(el, **kwargs))
        n_tasks_counter += 1

    send_request(scaffold_sender, ExpectedNItems(n_tasks_counter))

    if on_closure is not None:
        on_closure()
