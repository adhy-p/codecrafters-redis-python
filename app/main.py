from app.redis_server import RedisServer
import asyncio
import argparse


async def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-p", "--port", default=6379, type=int)
    arg_parser.add_argument("--replicaof", nargs="*")
    args = arg_parser.parse_args()
    if args.replicaof:
        host, port = args.replicaof
        rs = await RedisServer.new(args.port, (host, int(port)))
    else:
        rs = await RedisServer.new(args.port)
    await rs.serve()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("stopping server...")
