import argparse
import time
import random
from memcache import Client
from threading import Thread, Lock, Condition

MAX_NUMBER_OF_KEYS = 65536

latencies = []
finish_benchmark = False
lock_latencies = Lock()
threads_started = 0
thread_start = Condition()
benchmark_start = Condition()


def cmd_set(key: bin, client: Client) -> None:
    client.set(key, b"Some value")


def cmd_get(key: bin, client: Client) -> None:
    client.get(key)


def run(op, max_keys: int, client: Client) -> None:
    global latencies, real_started, threads_started

    local_latencies = []
    elapsed = None

    with thread_start:
        threads_started += 1
        thread_start.notify()

    with benchmark_start:
        benchmark_start.wait()

    while not finish_benchmark:
        key = random.randint(0, max_keys)
        start = time.monotonic()
        op(str(key).encode(), client)
        latency = time.monotonic() - start
        local_latencies.append(latency)

    lock_latencies.acquire()
    latencies += local_latencies
    lock_latencies.release()

def benchmark(desc: str, op, max_keys: int, client: Client, concurrency: int, duration: int) -> None:
    global finish_benchmark, real_started, latencies, threads_started

    finish_benchmark = False
    threads_started = 0
    latencies = []

    print("Starting threads ....")
    threads = []
    for idx in range(concurrency):
        thread = Thread(target=run, args=(op, max_keys, client))
        thread.start()
        threads.append(thread)

    def all_threads_started():
        return threads_started == concurrency

    # Wait till all of the threads are ready to start the benchmark
    with thread_start:
        thread_start.wait_for(all_threads_started)

    print("Running benchmark ....")
    # Signal the threads to start the benchmark
    with benchmark_start:
        benchmark_start.notify_all()

    time.sleep(duration)
    finish_benchmark = True

    for thread in threads:
        thread.join()

    latencies.sort()

    total_requests = len(latencies)
    avg = sum(latencies) / total_requests 
    p90 = latencies[int((90*total_requests)/100)]
    p99 = latencies[int((99*total_requests)/100)]

    print('QPS: {0}'.format(int(total_requests/duration)))
    print('Avg: {0:.6f}'.format(avg))
    print('P90: {0:.6f}'.format(p90))
    print('P99: {0:.6f}'.format(p99))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--memcache-address",
        help="Redis address, by default redis://localhost",
        default="localhost",
    )
    parser.add_argument(
        "--memcache-port",
        help="Memcache port, by default 11211",
        default=11211,
    )
    parser.add_argument(
        "--concurrency",
        help="Number of concurrency clients, by default 32",
        type=int,
        default=32,
    )
    parser.add_argument(
        "--duration",
        help="Test duration in seconds, by default 60",
        type=int,
        default=60,
    )
    args = parser.parse_args()

    client = Client(
        ["{}:{}".format(args.memcache_address, args.memcache_port)]
    )

    benchmark(
        "SET",
        cmd_set,
        MAX_NUMBER_OF_KEYS,
        client,
        args.concurrency,
        args.duration
    )

    benchmark(
        "GET",
        cmd_set,
        MAX_NUMBER_OF_KEYS,
        client,
        args.concurrency,
        args.duration
    )

