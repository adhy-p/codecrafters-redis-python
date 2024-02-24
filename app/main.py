from app.redis_server import RedisServer

def main():
    rs = RedisServer()
    rs.serve()


if __name__ == "__main__":
    main()
