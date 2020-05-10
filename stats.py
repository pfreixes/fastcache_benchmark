import asyncio
import argparse
import aiomcache
import random
import sys
import time
import uvloop

uvloop.install()


async def main(client: aiomcache.Client) -> None:
    while True:
        await asyncio.sleep(1)
        print((await client.stats())[b'curr_connections'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--memcache-address",
        help="Redis address, by default redis://localhost",
        default="127.0.0.1",
    )
    parser.add_argument(
        "--memcache-port",
        help="Memcache port, by default 11211",
        default=11211,
    )
    args = parser.parse_args()

    loop = asyncio.get_event_loop()

    client = aiomcache.Client(
        args.memcache_address,
        args.memcache_port,
        loop=loop
    )

    loop.run_until_complete(main(client))
