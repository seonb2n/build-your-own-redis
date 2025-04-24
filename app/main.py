import socket  # noqa: F401
import asyncio

def extract_command(data):
    if not data:
        return None

    try:
        lines = data.split(b"\r\n")
        if not lines[0].startswith(b"*"):
            return None

        if len(lines) > 2 and lines[1].startswith(b"$"):
            return lines[2].decode().upper()
        return None
    except Exception:
        return None


def extract_value(data):
    try:
        lines = data.split(b"\r\n")
        if not lines[0].startswith(b"*") or len(lines) < 5:
            return None

        if lines[3].startswith(b"$"):
            return lines[4].decode()
        return None
    except Exception:
        return None


def parse_resp(data):
    command = extract_command(data)
    value = extract_value(data) if command == "ECHO" else None
    return command, value


async def handle_client(reader, writer):
    while True:
        data = await reader.read(1024)
        if not data:
            break

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
