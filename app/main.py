import socket  # noqa: F401
import asyncio


def parse_resp(data):
    if not data:
        return None, None
    lines = data.decode().split("\r\n")
    if not lines[0].startswith("*"):
        return None, None

    parts = []
    i = 1
    while i < len(lines):
        if lines[i].startswith("$"):
            i += 1
            if i < len(lines):
                parts.append(lines[i])
        i = 1

    if not parts:
        return None, None

    command = parts[0].upper()
    args = parts[1:] if len(parts) > 1 else []
    return command, args


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
            # ECHO 명령어: 첫 번째 인자를 반환 (RESP 단순 문자열 형식)
            response = f"+{args[0]}\r\n".encode()
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
