import threading
from time import sleep
from kazoo.client import KazooClient
import random
from datetime import datetime

commit = b'commit'
abort = b'abort'


class Worker(threading.Thread):
    def __init__(self, id, root):
        super(Worker, self).__init__()

        self.id = id
        self.root = root

        self.path = f"{root}/{id}"

    def run(self):
        zk = KazooClient()
        zk.start()

        while not zk.exists(self.root):
            sleep(5)

        value = commit if random.random() > 0.25 else abort
        print(f"Node {self.id} vote {value.decode()}")
        zk.create(self.path, value, ephemeral=True)

        @zk.DataWatch(self.path)
        def watch(data, stat):
            if stat.version > 0:
                print(f"Node {self.id} perform {data.decode()}")

        sleep(10)
        zk.delete(self.path)
        zk.stop()
        zk.close()


class Master(threading.Thread):
    def __init__(self, number_of_workers):
        super(Master, self).__init__()

        self.number_of_workers = number_of_workers

        self.root = "/app"
        self.node_home = f"{self.root}/tx"
        self.strategies = [
            lambda c, a: commit if c > a else abort,
            lambda c, a: commit if a == 0 else abort
        ]

    def run(self):
        zk = KazooClient(hosts='localhost:2182')
        zk.start()
        zk.ensure_path(self.node_home)
        then = datetime.now()
        is_solved = [False]

        @zk.ChildrenWatch(self.node_home)
        def workers_watch(workers):
            if not is_solved[0]:
                if len(workers) == self.number_of_workers or (datetime.now() - then).total_seconds() > 30:
                    print("All workers voted")
                    workers = zk.get_children(self.node_home)
                    commits, aborts = 0, 0

                    for w in workers:
                        commits += int(zk.get(f"{self.node_home}/{w}")[0] == commit)
                        aborts += int(zk.get(f"{self.node_home}/{w}")[0] == abort)

                    decision = self.strategies[1](commits, aborts)

                    for w in workers:
                        zk.set(f"{self.node_home}/{w}", decision)

                    is_solved[0] = True
                else:
                    print(f"register nodes {workers}")

        sleep(30)
        zk.delete(self.node_home, recursive=True)
        zk.stop()
        zk.close()


if __name__ == "__main__":
    number_of_workers = 5

    threads = []

    master = Master(number_of_workers)
    thread = threading.Thread(target=master.run, daemon=False)
    threads.append(thread)

    for id in range(number_of_workers):
        worker = Worker(id, master.node_home)
        thread = threading.Thread(target=worker.run, daemon=False)
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
