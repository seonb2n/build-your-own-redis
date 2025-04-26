import socket  # noqa: F401
import asyncio
import time
import datetime

COMMANDS_WITH_ARGS = { "ECHO", "SET", "GET" }

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
        if not lines[0].startswith(b"*"):
            return None

        num_elements = int(lines[0][1:].decode())
        if num_elements < 2:
            return []

        args = []
        for i in range(3, len(lines), 2):
            if i + 1 < len(lines) and lines[i].startswith(b"$"):
                args.append(lines[i + 1].decode())
        return args
    except Exception:
        return None


def parse_resp(data):
    command = extract_command(data)
    value = extract_value(data) if command in COMMANDS_WITH_ARGS else []
    return command, value


async def handle_client(reader, writer):
    redis_map = dict()
    while True:
        data = await reader.read(1024)

        if not data:
            break

        command, args = parse_resp(data)

        if command == "PING":
            writer.write(b"+PONG\r\n")
        elif command == "ECHO" and args:
            response = f"+{args[0]}\r\n".encode()
            writer.write(response)
        elif command == "SET" and args:
            if args[2] is not None and args[2] == 'px':
                delta = datetime.timedelta(milliseconds=int(args[3]))
                redis_map[args[0]] = (args[1], datetime.datetime.now() + delta)
            else :
                redis_map[args[0]] = (args[1], -1)
            writer.write(b"+OK\r\n")
        elif command == "GET" and args:
            found_value = redis_map.get(args[0])
            print(found_value)
            if found_value[1] != -1 and found_value[1] < datetime.datetime.now():
                response = f"+{redis_map.get(args[0])}\r\n".encode()
                writer.write(response)
            else:
                writer.write(b"-1\r\n")
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
