import threading
import queue
import typing
import random
import time

# 4. Implement a Producer that will read numbers from the source container into the queue and notify the consumer when the queue is full.
class Producer(threading.Thread):
    def __init__(self, source: typing.List[typing.Union[int, float]], q: queue.Queue):
        super().__init__(name="Producer")
        self.source = source
        self.q = q

    def run(self):
        # store both index and item into the queue to maintain order
        for i, item in enumerate(self.source):
            self.q.put((i, item)) # internally blocks if the queue is full

            # Uncomment to see producer activity
            # print(f"Producer: put index={i}, value={item} (queue size={self.q.qsize()})")
        
        self.q.put((None, None)) # signal consumer to stop
        print("Producer finished.")


# 5. Create a consumer that will read from the queue into the destination container and notify the producer when the queue is empty.
class Consumer(threading.Thread):
    def __init__(self, destination: typing.List[typing.Union[int, float, None]], q: queue.Queue):
        super().__init__(name="Consumer")
        self.destination = destination
        self.q = q

    def run(self):
        while True:
            i_item = self.q.get() # internally blocks if the queue is empty
            
            if i_item == (None, None):
                self.q.task_done()
                break

            i, item = i_item
            self.destination[i] = item

            # Uncomment to see consumer activity
            # print(f"Consumer: got index={i}, value={item} (queue size={self.q.qsize()})")

            self.q.task_done() # mark the item as processed

        print("Consumer finished.")


def main():
    # 1. Create a source container and populate it with integers and doubles.
    SOURCE_SIZE = 10
    source: typing.List[typing.Union[int, float]] = [
        random.choice([random.randint(1, 100), round(random.random() * 100, 4)]) for _ in range(SOURCE_SIZE)
    ]

    # 2. Create a destination container that can store both integers and doubles with the same capacity as the source container from task 1.
    destination: typing.List[typing.Union[int, float, None]] = [None] * len(source)

    # 3. Create a queue with half the capacity of the source container from task 1.
    q = queue.Queue(maxsize=len(source) // 2)

    # Create and start producer/consumer threads
    producer = Producer(source, q)
    consumer = Consumer(destination, q)

    start_time = time.perf_counter()
    
    producer.start()
    consumer.start()

    # wait for both threads to finish
    producer.join()
    consumer.join()

    end_time = time.perf_counter()

    # 6. Write a test to confirm the numbers from the source container have been copied to the destination container.
    print("Test passed!" if source == destination else "Test failed!")
    print(f"Source:      {source}")
    print(f"Destination: {destination}")
    print(f"Total execution time: {end_time - start_time:.4f} seconds.")


if __name__ == "__main__":
    main()
