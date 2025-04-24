import socket  # noqa: F401
import asyncio


def parse_resp(data):
    [_, _, command, _, value] = data.strip().split(b"\r\n")
    return command.upper(), value


async def handle_client(reader, writer):
    while True:
        data = await reader.read(1024)
        if not data:
            break

        # RESP 데이터 파싱
        command, args = parse_resp(data)

        if command == "PING":
            writer.write(b"+PONG\r\n")
        elif command == "ECHO" and args:
            response = f"+{args}\r\n".encode()
            writer.write(response)
        else:
            # 지원하지 않는 명령어 또는 잘못된 형식
            writer.write(b"-ERR unknown command or invalid arguments\r\n")

        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    print("Logs from your program will appear here!")

    server = await asyncio.start_server(handle_client, "localhost", 6379, reuse_port=True)

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
