"""
Unit test for producer_consumer module.

Verifies that the producer-consumer workflow correctly transfers data
from a source list to a destination list using Python threads and a queue.
"""

import queue
import threading
from producer_consumer import producer, consumer

def test_producer_consumer():
    """
    Verify that the producer and consumer threads transfer all data correctly.

    Steps
    -----
    1. Create a small sample dataset.
    2. Initialize a bounded queue to simulate blocking behavior.
    3. Start producer and consumer threads.
    4. Wait for both threads to complete.
    5. Assert that the destination matches the source.
    """
    source = [1, 2, 3, 4, 5]
    destination = [None] * len(source)
    q = queue.Queue(maxsize=2)

    producer_thread = threading.Thread(target=producer, args=(source, q), name="ProducerTest")
    consumer_thread = threading.Thread(target=consumer, args=(destination, q), name="ConsumerTest")

    producer_thread.start()
    consumer_thread.start()
    producer_thread.join()
    consumer_thread.join()

    assert source == destination, "Destination list does not match source data"
