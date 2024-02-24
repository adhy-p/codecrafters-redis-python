from app.redis_server import RedisServer
import asyncio


async def main():
    rs = await RedisServer.new()
    await rs.serve()


if __name__ == "__main__":
    asyncio.run(main())
