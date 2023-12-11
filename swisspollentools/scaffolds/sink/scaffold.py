"""
Sink Scaffold for Closing a Pipeline.

The Sink scaffold is used to close a pipeline by receiving responses from the
last layer of workers in an HPC (High-Performance Computing) environment. It
employs PyZMQ for communication and utilizes utility functions from the
SwissPollenTools library.
"""
import time
from typing import Callable, Optional

import zmq

from swisspollentools.utils import \
    LAUNCH_SLEEP_TIME, N_ITEMS_KEY, \
    send_request, recv_request, \
    EndOfProcess, isexnit, iseot

def Sink(
    pull_port: int,
    control_port: int,
    scaffold_port: int,
    on_startup: Optional[Callable]=None,
    on_closure: Optional[Callable]=None,
    **kwargs
):
    """
    Create and run a Sink scaffold for closing a pipeline and coordinating
    workers.

    Parameters:
    - pull_port (int): The port for receiving data from the last layer of
    workers.
    - control_port (int): The port for control communication (PUB/SUB channel).
    - scaffold_port (int): The port for scaffold communication.
    - on_startup (Optional[Callable]): An optional callback function to execute
    on scaffold startup.
    - on_closure (Optional[Callable]): An optional callback function to execute
    on scaffold closure.
    - **kwargs (Any): Additional keyword arguments.

    Returns:
    None

    Example:
    # Example usage of the Sink scaffold
    from swisspollentools.sink import Sink

    Sink(
        pull_port=5555,
        control_port=5556,
        scaffold_port=5557,
        on_startup=lambda: print("Sink scaffold started"),
        on_closure=lambda: print("Sink scaffold closed")
    )

    Note:
    - The Sink scaffold creates ZeroMQ sockets for communication, receives
    responses from the last layer of workers, handles control messages for
    efficient pipeline coordination, and signals the end of the process when
    the expected number of tasks is received.
    """

    if on_startup is not None:
        on_startup()

    context = zmq.Context()

    # Set PULL binding
    receiver = context.socket(zmq.PULL)
    receiver.bind(f"tcp://127.0.0.1:{pull_port}")

    # Set control binding (PUB/SUB channel)
    control = context.socket(zmq.PUB)
    control.bind(f"tcp://127.0.0.1:{control_port}")

    scaffold_receiver = context.socket(zmq.PAIR)
    scaffold_receiver.connect(f"tcp://127.0.0.1:{scaffold_port}")

    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)
    poller.register(scaffold_receiver, zmq.POLLIN)

    time.sleep(LAUNCH_SLEEP_TIME)

    n_tasks = float("inf")
    eot_counter = 0
    while eot_counter < n_tasks:
        socks = dict(poller.poll())

        if socks.get(receiver) == zmq.POLLIN:
            request = recv_request(receiver)

            if iseot(request):
                eot_counter += 1

        if socks.get(scaffold_receiver) == zmq.POLLIN:
            request = recv_request(scaffold_receiver)

            if isexnit(request):
                n_tasks = request[N_ITEMS_KEY]

    send_request(control, EndOfProcess())

    if on_closure is not None:
        on_closure()
