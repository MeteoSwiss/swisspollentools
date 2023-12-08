import time
import zmq

from typing import Callable, Optional, Tuple

from swisspollentools.utils import *

def Parallel(
    pull_port: int,
    push_ports: int,
    scaffold_ports: Tuple[int],
    on_startup: Optional[Callable]=None,
    on_closure: Optional[Callable]=None,
    **kwargs
):
    if not len(push_ports) == len(scaffold_ports) - 1:
        raise ValueError()

    if on_startup is not None:
        on_startup()

    context = zmq.Context()

    receiver = context.socket(zmq.PULL)
    receiver.connect(f"tcp://127.0.0.1:{pull_port}")

    senders = [context.socket(zmq.PUSH) for _ in push_ports]
    [sender.bind(f"tcp://127.0.0.1:{push_port}") \
        for sender, push_port in zip(senders, push_ports)]
    
    scaffold_receiver = context.socket(zmq.PAIR)
    scaffold_receiver.connect(f"tcp://127.0.0.1:{scaffold_ports[0]}")

    scaffold_senders = [context.socket(zmq.PAIR) for _ in scaffold_ports[1:]]
    [sender.bind(f"tcp://127.0.0.1:{scaffold_port}") \
        for sender, scaffold_port in zip(scaffold_senders, scaffold_ports[1:])]
    
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

    return
