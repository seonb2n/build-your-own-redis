import socket  # noqa: F401
import asyncio

from app.RedisServer import RedisServer


async def main() -> None:
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Redis Server')
    parser.add_argument('--dir', default='/tmp', help='Directory for RDB file')
    parser.add_argument('--dbfilename', default='dump.rdb', help='RDB filename')
    parser.add_argument('--port', default=6379, type=int, help='Redis server port')
    parser.add_argument('--replicaof', help='Redis server replicaof')
    args = parser.parse_args()

    print(f"Using dir: {args.dir}, dbfilename: {args.dbfilename}, port: {args.port}, replicaof: {args.replicaof}")

    server = RedisServer(dir_path=args.dir, dbfilename=args.dbfilename, replicaof=args.replicaof, port=args.port)
    redis_server = await asyncio.start_server(
        server.handle_client, "localhost", args.port, reuse_port=True
    )

    await server.async_init()

    async with redis_server:
        await redis_server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
