import socket  # noqa: F401
import asyncio
import datetime
from typing import Dict, List, Optional, Tuple, Union, Any

CRLF = b"\r\n"
RESP_ARRAY_PREFIX = b"*"
RESP_BULK_STRING_PREFIX = b"$"
RESP_SIMPLE_STRING_PREFIX = b"+"
RESP_ERROR_PREFIX = b"-"


# 지원하는 명령어
class Commands:
    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"


class RespBuilder:
    @staticmethod
    def simple_string(value: str) -> bytes:
        return f"{RESP_SIMPLE_STRING_PREFIX.decode()}{value}{CRLF.decode()}".encode()

    @staticmethod
    def error(message: str) -> bytes:
        return f"{RESP_ERROR_PREFIX.decode()}{message}{CRLF.decode()}".encode()

    @staticmethod
    def null() -> bytes:
        return b"$-1\r\n"


class RespParser:
    @staticmethod
    def parse(data: bytes) -> Tuple[Optional[str], List[str]]:
        try:
            lines = data.split(CRLF)

            # 배열 타입 확인
            if not lines[0].startswith(RESP_ARRAY_PREFIX):
                return None, []

            # 배열 요소 개수 확인
            num_elements = int(lines[0][1:].decode())
            if num_elements < 1:
                return None, []

            # 명령어 추출
            if len(lines) > 2 and lines[1].startswith(RESP_BULK_STRING_PREFIX):
                command = lines[2].decode().upper()
            else:
                return None, []

            # 인자 추출
            args = []
            for i in range(3, len(lines), 2):
                if i + 1 < len(lines) and lines[i].startswith(RESP_BULK_STRING_PREFIX):
                    args.append(lines[i + 1].decode())

            return command, args
        except Exception:
            return None, []

class RedisStore:
    def __init__(self):
        self.data: Dict[str, Tuple[str, Union[int, datetime.datetime]]] = {}

    def set(self, key: str, value: str, expiry: Optional[datetime.datetime] = None) -> None:
        self.data[key] = (value, expiry if expiry else -1)

    def get(self, key: str) -> Optional[str]:
        if key not in self.data:
            return None

        value, expiry = self.data[key]

        # 만료 시간 확인
        if expiry != -1 and datetime.datetime.now() >= expiry:
            del self.data[key]
            return None

        return value

class RedisServer:
    def __init__(self):
        self.store = RedisStore()
        self.parser = RespParser()
        self.builder = RespBuilder()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break

                command, args = self.parser.parse(data)
                response = self.handle_command(command, args)

                writer.write(response)
                await writer.drain()
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    def handle_command(self, command: Optional[str], args: List[str]) -> bytes:
        if command is None:
            return self.builder.error("ERR protocol error")

        handlers = {
            Commands.PING: self.handle_ping,
            Commands.ECHO: self.handle_echo,
            Commands.SET: self.handle_set,
            Commands.GET: self.handle_get
        }

        handler = handlers.get(command)
        if handler:
            return handler(args)

        return self.builder.error("ERR unknown command or invalid arguments")

    def handle_ping(self, args: List[str]) -> bytes:
        return self.builder.simple_string("PONG")

    def handle_echo(self, args: List[str]) -> bytes:
        if not args:
            return self.builder.error("ERR wrong number of arguments for 'echo' command")
        return self.builder.simple_string(args[0])

    def handle_set(self, args: List[str]) -> bytes:
        if len(args) < 2:
            return self.builder.error("ERR wrong number of arguments for 'set' command")

        key, value = args[0], args[1]
        expiry = None

        # px 옵션 처리
        if len(args) > 2 and args[2].lower() == 'px' and len(args) > 3:
            try:
                milliseconds = int(args[3])
                expiry = datetime.datetime.now() + datetime.timedelta(milliseconds=milliseconds)
            except ValueError:
                return self.builder.error("ERR value is not an integer or out of range")

        self.store.set(key, value, expiry)
        return self.builder.simple_string("OK")

    def handle_get(self, args: List[str]) -> bytes:
        if not args:
            return self.builder.error("ERR wrong number of arguments for 'get' command")

        value = self.store.get(args[0])
        if value is None:
            return self.builder.null()

        return self.builder.simple_string(value)


async def main() -> None:
    print("Redis server starting on localhost:6379")
    server = RedisServer()
    redis_server = await asyncio.start_server(
        server.handle_client, "localhost", 6379, reuse_port=True
    )

    async with redis_server:
        await redis_server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())