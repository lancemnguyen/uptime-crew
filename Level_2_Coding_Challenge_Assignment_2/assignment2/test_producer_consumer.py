"""
Unit tests for producer_consumer module.

Covers various input scenarios to verify that the producer-consumer workflow
correctly transfers data between threads using a shared queue.
"""

import unittest
import threading
import queue
from producer_consumer import Producer, Consumer


class TestProducerConsumer(unittest.TestCase):
    """Test suite for producer-consumer threading workflow."""

    def run_test_case(self, source):
        """Helper to execute producer-consumer logic and return destination."""
        destination = [None] * len(source)
        q = queue.Queue(maxsize=max(1, len(source) // 2))  # simulate bounded queue

        producer_thread = Producer(source=source, q=q, name="ProducerTest")
        consumer_thread = Consumer(destination=destination, q=q, name="ConsumerTest")

        producer_thread.start()
        consumer_thread.start()
        producer_thread.join()
        consumer_thread.join()

        return destination

    def test_even_number_of_items(self):
        """Verify that even-sized lists are transferred correctly."""
        source = [1, 2, 3, 4]
        destination = self.run_test_case(source)
        self.assertEqual(source, destination)

    def test_odd_number_of_items(self):
        """Verify that odd-sized lists are transferred correctly."""
        source = [10, 20, 30, 40, 50]
        destination = self.run_test_case(source)
        self.assertEqual(source, destination)

    def test_single_element(self):
        """Verify single-element list transfers correctly."""
        source = [42]
        destination = self.run_test_case(source)
        self.assertEqual(source, destination)

    def test_empty_list(self):
        """Verify that empty lists are handled gracefully."""
        source = []
        destination = self.run_test_case(source)
        self.assertEqual(source, destination)

    def test_large_dataset(self):
        """Verify correctness with a large dataset."""
        source = list(range(1000))
        destination = self.run_test_case(source)
        self.assertEqual(source, destination)

    def test_negative_and_float_values(self):
        """Verify handling of mixed numeric types."""
        source = [-1, -2.5, 0, 3.14, 10]
        destination = self.run_test_case(source)
        self.assertEqual(source, destination)

    def test_duplicate_values(self):
        """Verify that duplicate values are preserved."""
        source = [5, 5, 3, 3, 3, 2, 2, 1]
        destination = self.run_test_case(source)
        self.assertEqual(source, destination)

    def test_order_preservation(self):
        """Ensure the order of elements is preserved."""
        source = [9, 7, 5, 3, 1]
        destination = self.run_test_case(source)
        self.assertEqual(source, destination)


if __name__ == "__main__":
    print("--- Running Producer-Consumer Unit Tests ---")
    unittest.main(verbosity=2)
