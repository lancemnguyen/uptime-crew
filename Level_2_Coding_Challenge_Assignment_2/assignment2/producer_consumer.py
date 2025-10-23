"""
Producer-Consumer Example using Python Threads and Queue

This module demonstrates a classic producer–consumer pattern implemented with
Python's standard `threading` and `queue` modules. A producer thread generates
random numeric data and places it into a thread-safe queue, while a consumer
thread retrieves items from the queue and stores them in a destination list.

Design highlights:
- Uses `queue.Queue` for built-in thread synchronization (no manual locks).
- Demonstrates blocking behavior when the queue is full or empty.
- Uses a sentinel value `(None, None)` to signal completion to the consumer.
- Implements structured logging for clarity and debugging.
- Includes validation and timing measurements in the main workflow.
"""

import threading
import queue
import random
import time
import logging
from typing import List, Optional, Union

Number = Union[int, float]

# Configure logging (INFO for normal output, DEBUG for verbose tracing)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S",
)

class Producer(threading.Thread):
    """
    Producer thread class.

    Sequentially reads items from a source list and places them into a shared
    queue along with their indices. This function blocks when the queue is
    full, ensuring back-pressure between producer and consumer.

    After producing all items, a sentinel value `(None, None)` is added to
    indicate that production is complete.

    Parameters:
    -----------
    source : List[Number]
        The list of numeric values to be produced.
    q : queue.Queue
        The shared queue used to transfer data between threads.
    name : str
        The name of the producer thread.
    """
    def __init__(self, source: List[Number], q: queue.Queue, name="Producer"):
        super().__init__(name=name)
        self.source = source
        self.q = q

    def run(self):
        try:
            for i, item in enumerate(self.source):
                self.q.put((i, item))  # blocks if queue is full
                logging.debug(f"Produced index={i}, value={item}, queue size={self.q.qsize()}")

            # Signal to the consumer that production is complete
            self.q.put((None, None))
            logging.info("Producer finished producing all items.")
        except Exception as e:
            logging.error(f"Producer error: {e}")


class Consumer(threading.Thread):
    """
    Consumer thread class.

    Continuously retrieves (index, item) pairs from the shared queue and writes
    each item into the corresponding position of the destination list. The
    consumer terminates when it encounters the sentinel `(None, None)`.

    Parameters
    ----------
    destination : List[Optional[Number]]
        The list that receives items consumed from the queue.
    q : queue.Queue
        The shared queue from which data is consumed.
    name : str
        The name of the consumer thread.
    """
    def __init__(self, destination: List[Optional[Number]], q: queue.Queue, name="Consumer"):
        super().__init__(name=name)
        self.destination = destination
        self.q = q
    

    def run(self):
        try:
            while True:
                i_item = self.q.get()  # blocks if queue is empty

                if i_item == (None, None):
                    self.q.task_done()
                    break

                i, item = i_item
                self.destination[i] = item
                logging.debug(f"Consumed index={i}, value={item}, queue size={self.q.qsize()}")
                self.q.task_done()

            logging.info("Consumer finished consuming all items.")
        except Exception as e:
            logging.error(f"Consumer error: {e}")

def main():
    """
    Run a demonstration of the producer–consumer workflow.

    This function initializes a random dataset, starts the producer and
    consumer threads, waits for both to complete, and verifies that all
    data was transferred correctly.
    """
    size = 10

    # Create a list of random integers and floats.
    source = [random.choice([random.randint(1, 100), random.random() * 100]) for _ in range(size)]
    destination = [None] * size

    # Initialize a bounded queue to simulate back-pressure.
    q = queue.Queue(maxsize=max(1, size // 2))  # half-capacity queue

    start = time.perf_counter()

    # Launch producer and consumer threads.
    threads = [
        Producer(source=source, q=q, name="Producer"),
        Consumer(destination=destination, q=q, name="Consumer")
    ]

    for t in threads:
        t.start()

    # Wait for threads to complete.
    for t in threads:
        t.join()

    elapsed = time.perf_counter() - start

    # Validate results and log execution time.
    assert source == destination, "Data mismatch between source and destination"
    logging.info(f"All data transferred successfully in {elapsed:.4f}s")

if __name__ == "__main__":
    main()
