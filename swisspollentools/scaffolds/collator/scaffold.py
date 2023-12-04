import time

import zmq

from typing import Callable, Optional, Tuple

from swisspollentools.utils import *

def Collator(
    request_fn: Callable,
    pull_port: int,
    push_port: int,
    control_port: int,
    scaffold_ports: Tuple[int],
    on_startup: Optional[Callable]=None,
    on_closure: Optional[Callable]=None,
    **kwargs
):
    if on_startup is not None:
        on_startup()

    context = zmq.Context()

    # Set PULL binding
    receiver = context.socket(zmq.PULL)
    receiver.bind(f"tcp://127.0.0.1:{pull_port}")

    # Set PUSH binding
    sender = context.socket(zmq.PUSH)
    sender.bind(f"tcp://127.0.0.1:{push_port}")

    # Set control binding (PUB/SUB channel)
    control = context.socket(zmq.PUB)
    control.bind(f"tcp://127.0.0.1:{control_port}")

    scaffold_receiver = context.socket(zmq.PAIR)
    scaffold_receiver.connect(f"tcp://127.0.0.1:{scaffold_ports[0]}")

    scaffold_sender = context.socket(zmq.PAIR)
    scaffold_sender.bind(f"tcp://127.0.0.1:{scaffold_ports[1]}")

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
            request = receiver.recv_json()

            if iseot(request):
                eot_counter += 1
                continue

            request = request_fn(
                file_path=request[FILE_PATH_KEY],
                batch_id=request[BATCH_ID_KEY],
                response=request, 
                **kwargs
            )
            sender.send_json(request)
            n_tasks_counter += 1

        if socks.get(scaffold_receiver) == zmq.POLLIN:
            request = scaffold_receiver.recv_json()

            if isexnit(request):
                n_tasks = request[N_ITEMS_KEY]

    scaffold_sender.send_json(ExpectedNItems(n_tasks_counter))
    control.send_json(EndOfProcess())

    if on_closure is not None:
        on_closure()

    return
