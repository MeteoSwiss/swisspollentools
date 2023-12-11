"""
SwissPollenTools Scaffolds Module

The `scaffolds` module in the SwissPollenTools library provides a collection of
scaffolds that facilitate the coordination and orchestration of tasks in a
High-Performance Computing (HPC) environment. These scaffolds are designed to
create pipelines for HPC devices using PyZMQ and utilize utility functions from
the SwissPollenTools library.

Available Scaffolds:
- Collator: Intermediate component for many-to-many cardinality.
- Parallel: Scaffold for running multiple tasks in parallel on a single
message.
- Sink: Scaffold for closing a pipeline by receiving responses from the last
layer of workers.
- Ventilator: Scaffold for starting a pipeline by sending requests built on
top of an iterable.

Each scaffold serves a specific role in the pipeline and can be combined to
create complex workflows for data processing, analysis, and coordination.
"""

from swisspollentools.scaffolds.collator.scaffold import Collator
from swisspollentools.scaffolds.parallel.scaffold import Parallel
from swisspollentools.scaffolds.sink.scaffold import Sink
from swisspollentools.scaffolds.ventilator.scaffold import Ventilator
