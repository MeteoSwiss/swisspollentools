"""
Parallel Scaffold for Running Multiple Tasks in Parallel on a Single Message.

The Parallel scaffold is designed to run multiple tasks in parallel on a single
message in an HPC (High-Performance Computing) environment. It uses PyZMQ for
communication and leverages utility functions from the SwissPollenTools
library.
"""

import time
from typing import Callable, Optional, Tuple

import zmq

from swisspollentools.utils import \
    LAUNCH_SLEEP_TIME, N_ITEMS_KEY, \
    recv_request, send_request, \
    ExpectedNItems, isexnit, iseot

def Parallel(
    pull_port: int,
    push_ports: int,
    scaffold_ports: Tuple[int],
    on_startup: Optional[Callable]=None,
    on_closure: Optional[Callable]=None,
    **kwargs
):
    """
    Create and run a Parallel scaffold for processing tasks in parallel on a
    single message.

    Parameters:
    - pull_port (int): The port for receiving data from upstream components.
    - push_ports (List[int]): The list of ports for sending processed data to
    downstream components.
    - scaffold_ports (Tuple[int]): A tuple containing ports for scaffold
    communication.
    - on_startup (Optional[Callable]): An optional callback function to execute
    on scaffold startup.
    - on_closure (Optional[Callable]): An optional callback function to execute
    on scaffold closure.
    - **kwargs (Any): Additional keyword arguments.

    Returns:
    None

    Example:
    # Example usage of the Parallel scaffold
    from swisspollentools.parallel import Parallel

    Parallel(
        pull_port=5555,
        push_ports=[5556, 5557],
        scaffold_ports=(5558, 5559),
        on_startup=lambda: print("Parallel scaffold started"),
        on_closure=lambda: print("Parallel scaffold closed")
    )

    Note:
    The Parallel scaffold creates ZeroMQ sockets for communication, runs
    multiple tasks in parallel on a single message, and handles control
    messages for efficient pipeline coordination.
    """
    if not len(push_ports) == len(scaffold_ports) - 1:
        raise ValueError()

    if on_startup is not None:
        on_startup()

    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.connect(f"tcp://127.0.0.1:{pull_port}")

    senders = [context.socket(zmq.PUSH) for _ in push_ports]
    for sender, push_port in zip(senders, push_ports):
        sender.bind(f"tcp://127.0.0.1:{push_port}")

    scaffold_receiver = context.socket(zmq.PAIR)
    scaffold_receiver.connect(f"tcp://127.0.0.1:{scaffold_ports[0]}")

    scaffold_senders = [context.socket(zmq.PAIR) for _ in scaffold_ports[1:]]
    for sender, scaffold_port in zip(scaffold_senders, scaffold_ports[1:]):
        sender.bind(f"tcp://127.0.0.1:{scaffold_port}")

    poller = zmq.Poller()
    poller.register(receiver, zmq.POLLIN)
    poller.register(scaffold_receiver, zmq.POLLIN)

    time.sleep(LAUNCH_SLEEP_TIME)

    n_tasks = float("inf")
    eot_counter = 0
    n_tasks_counter = 0
    while eot_counter < n_tasks:
        socks = dict(poller.poll())

        if socks.get(receiver) == zmq.POLLIN:
            request = recv_request(receiver)

            if iseot(request):
                eot_counter += 1
                continue

            for sender in senders:
                send_request(sender, request)
            n_tasks_counter += 1

        if socks.get(scaffold_receiver) == zmq.POLLIN:
            request = recv_request(scaffold_receiver)

            if isexnit(request):
                n_tasks = request[N_ITEMS_KEY]

    for scaffold_sender in scaffold_senders:
        send_request(scaffold_sender, ExpectedNItems(n_tasks_counter))

    if on_closure is not None:
        on_closure()
