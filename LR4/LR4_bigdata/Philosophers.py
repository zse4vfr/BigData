import threading
import time
import random
from kazoo.client import KazooClient


class Philosopher(threading.Thread):
    def __init__(self, id, address, root, left, right, seats, mutex, iters):
        super(Philosopher, self).__init__()

        self.id = id
        self.address = address
        self.root = root
        self.left = left
        self.right = right
        self.seats = seats
        self.mutex = mutex
        self.iter_max = iters

        self.cur_iter = None
        self.zk = KazooClient(hosts=address)
        self.path = f"{root}/{id}"

    def run(self):
        self.zk.start()
        for i in range(self.iter_max):
            self.cur_iter = i
            self.eat()
            self.think()

    def eat(self):
        print(f"Philosopher {self.id} is going to eat")
        while True:
            if len(self.zk.get_children(self.root)) <= self.seats:
                self.zk.create(self.path, b"", ephemeral=True)

                self.mutex.acquire()
                self.left.acquire()
                print(f"Philosopher {self.id} picked up the left fork")
                self.right.acquire()
                print(f"Philosopher {self.id} picked up the right fork, eating")
                self.mutex.release()

                time.sleep((random.randint(0, 4) + 1) * 2)

                self.right.release()
                print(f"Philosopher {self.id} put back the right fork")
                self.left.release()
                print(f"Philosopher {self.id} put back the left fork, finished eating")

                break

    def think(self):
        print(f"Philosopher {self.id} is thinking")
        time.sleep((random.randint(0, 4) + 1) * 2)
        self.zk.delete(self.path)
        print(f"Philosopher {self.id} finished thinking")
        if self.cur_iter == self.iter_max - 1:
            self.zk.stop()
            self.zk.close()


if __name__ == "__main__":
    iters = 2
    address = "localhost:2182"
    philosophers_count = 5
    seats = philosophers_count
    mutex = threading.Semaphore(philosophers_count - 1)
    forks = [threading.Semaphore(1) for _ in range(philosophers_count)]
    threads = []

    zk = KazooClient(hosts=address)
    zk.start()
    if not zk.exists("/philosophers"):
        zk.create("/philosophers")
    zk.stop()
    zk.close()

    for id in range(philosophers_count):
        philosopher = Philosopher(id,
                                  address,
                                  "/philosophers",
                                  forks[id],
                                  forks[(id + 1) % philosophers_count],
                                  seats,
                                  mutex,
                                  iters)
        thread = threading.Thread(target=philosopher.run, daemon=False)
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

