import threading
import queue
import typing
from typing import Optional
import random
import time

Number = typing.Union[int, float]
DEBUG = False

# 4. Implement a Producer that will read numbers from the source container into the queue and notify the consumer when the queue is full.
class Producer(threading.Thread):
    def __init__(self, source: typing.List[Number], q: queue.Queue):
        super().__init__(name="Producer")
        self.source = source
        self.q = q

    def run(self):
        try:
            # store both index and item into the queue to maintain order
            for i, item in enumerate(self.source):
                self.q.put((i, item)) # internally blocks if the queue is full

                # producer activity
                if DEBUG:
                    print(f"Producer: put index={i}, value={item} (queue size={self.q.qsize()})")
            
            self.q.put((None, None)) # signal consumer to stop
            print("Producer finished.")

        except Exception as e:
            print(f"Producer error: {e}")


# 5. Create a consumer that will read from the queue into the destination container and notify the producer when the queue is empty.
class Consumer(threading.Thread):
    def __init__(self, destination: typing.List[Optional[Number]], q: queue.Queue):
        super().__init__(name="Consumer")
        self.destination = destination
        self.q = q

    def run(self):
        try:
            while True:
                i_item = self.q.get() # internally blocks if the queue is empty
                
                if i_item == (None, None):
                    self.q.task_done()
                    break

                i, item = i_item
                self.destination[i] = item

                # consumer activity
                if DEBUG:
                    print(f"Consumer: got index={i}, value={item} (queue size={self.q.qsize()})")

                self.q.task_done() # mark the item as processed

            print("Consumer finished.")

        except Exception as e:
            print(f"Consumer error: {e}")


def main():
    # 1. Create a source container and populate it with integers and doubles.
    SOURCE_SIZE = 10
    source: typing.List[Number] = [
        random.choice([random.randint(1, 100), round(random.random() * 100, 4)]) for _ in range(SOURCE_SIZE)
    ]
    
    if not source:
        print("Empty source, nothing to produce or consume.")
        return

    # 2. Create a destination container that can store both integers and doubles with the same capacity as the source container from task 1.
    destination: typing.List[Optional[Number]] = [None] * len(source)

    # 3. Create a queue with half the capacity of the source container from task 1.
    q = queue.Queue(maxsize=max(1, len(source) // 2))

    try:
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
    
    except Exception as e:
        print(f"Main thread error: {e}")


if __name__ == "__main__":
    main()
