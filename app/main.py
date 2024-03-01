from app.redis_server import RedisMasterServer, RedisWorkerServer
import asyncio
import argparse


async def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-p", "--port", default=6379, type=int)
    arg_parser.add_argument("--dir", default=".")
    arg_parser.add_argument("--dbfilename", default="data.rdb")
    arg_parser.add_argument("--replicaof", nargs="*")
    args = arg_parser.parse_args()
    config = {
        "port": args.port,
        "dir": args.dir,
        "dbfilename": args.dbfilename,
    }
    if args.replicaof:
        host, port = args.replicaof
        config.update({"master": (host, int(port))})
        rs = await RedisWorkerServer.new(config)
    else:
        rs = await RedisMasterServer.new(config)
    await rs.serve()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("stopping server...")
