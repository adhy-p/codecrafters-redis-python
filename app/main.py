from app.redis_server import RedisServer
import asyncio
import argparse


async def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-p", "--port", default=6379, type=int)
    args = arg_parser.parse_args()
    rs = await RedisServer.new(args.port)
    await rs.serve()


if __name__ == "__main__":
    asyncio.run(main())
