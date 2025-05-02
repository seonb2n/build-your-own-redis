import os.path
import socket  # noqa: F401
import asyncio
import datetime
from typing import Dict, List, Optional, Tuple, Union, Any

from app.RdbParse import RdbParser

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
    CONFIG = "CONFIG"
    KEYS = "KEYS"
    INFO = "INFO"


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

    @staticmethod
    def bulk_string(value: str) -> bytes:
        encoded_value = value.encode()
        return f"{RESP_BULK_STRING_PREFIX.decode()}{len(encoded_value)}{CRLF.decode()}{value}{CRLF.decode()}".encode()

    @staticmethod
    def array(items: List[bytes]) -> bytes:
        result = f"{RESP_ARRAY_PREFIX.decode()}{len(items)}{CRLF.decode()}".encode()
        for item in items:
            result += item
        return result


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
        print(expiry)

        # 만료 시간 확인
        if expiry != -1 and datetime.datetime.now() >= expiry:
            del self.data[key]
            return None

        return value

    def get_all_keys(self) -> List[str]:
        """만료되지 않은 모든 키 목록 반환"""
        now = datetime.datetime.now()
        result = []

        for key, (_, expiry) in list(self.data.items()):
            # 만료 시간 확인
            if expiry == -1 or now < expiry:
                result.append(key)
            else:
                # 만료된 키 삭제
                del self.data[key]

        return result


class RedisServer:
    def __init__(self, dir_path="/tmp", dbfilename="dump.rdb", replicaof: Optional[str] = None, port: int = 6379):
        self.store = RedisStore()
        self.parser = RespParser()
        self.builder = RespBuilder()
        self.config = {
            "dir": dir_path,
            "dbfilename": dbfilename,
            "replicaof": "slave" if replicaof else "master",
            "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        }
        self.master_repl_offset = 0
        self.master_host: Optional[str] = None
        self.master_port: Optional[int] = None
        self.port = port
        self._replicaof = replicaof

        self._load_rdb()

    async def async_init(self):
        if self._replicaof:
            try:
                self.master_host, port_str = self._replicaof.split()
                self.master_port = int(port_str)
                await self.connect_to_master()
            except ValueError:
                raise ValueError("Invalid replicaof format. Expected 'host port'.")

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

    async def connect_to_master(self):
        if not (self.master_host and self.master_port):
            return
        try:
            reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
            # step 1: Send PING
            ping_command = self.builder.array([self.builder.bulk_string("PING")])
            writer.write(ping_command)
            await writer.drain()

            response = await reader.read(1024)

            # step 2: send REPLCONF
            replconf_port = self.builder.array(
                [
                    self.builder.bulk_string("REPLCONF"),
                    self.builder.bulk_string("listening-port"),
                    self.builder.bulk_string(str(self.port))
                ]
            )
            writer.write(replconf_port)
            await writer.drain()
            response = await reader.read(1024)

            # Step 3: Send REPLCONF capa psync2
            replconf_capa = self.builder.array([
                self.builder.bulk_string("REPLCONF"),
                self.builder.bulk_string("capa"),
                self.builder.bulk_string("psync2")
            ])
            writer.write(replconf_capa)
            await writer.drain()

            response = await reader.read(1024)

            # Step 4: Send PSYNC
            psync = self.builder.array([
                self.builder.bulk_string("psync"),
                self.builder.bulk_string("?"),
                self.builder.bulk_string("-1")
            ])
            writer.write(psync)
            await writer.drain()

            response = await reader.read(1024)

            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"Failed to send PING to master: {e}")

    def handle_command(self, command: Optional[str], args: List[str]) -> bytes:
        if command is None:
            return self.builder.error("ERR protocol error")

        handlers = {
            Commands.PING: self.handle_ping,
            Commands.ECHO: self.handle_echo,
            Commands.SET: self.handle_set,
            Commands.GET: self.handle_get,
            Commands.CONFIG: self.handle_config,
            Commands.KEYS: self.handle_keys,
            Commands.INFO: self.handle_info,
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

    def handle_config(self, args: List[str]) -> bytes:
        if len(args) < 2:
            return self.builder.error("ERR wrong number of arguments for 'config' command")

        subcommand = args[0].upper()
        if subcommand != "GET":
            return self.builder.error("ERR unknown subcommand for CONFIG")

        param_name = args[1]
        if param_name not in self.config:
            return self.builder.array([])

        return self.builder.array([
            self.builder.bulk_string(param_name),
            self.builder.bulk_string(self.config[param_name])
        ])

    def handle_info(self, args: List[str]) -> bytes:
        replication = f"role:{self.config['replicaof']}"
        master_replid = f"master_replid:{self.config['master_replid']}"
        master_repl_offset = f"master_repl_offset:{self.master_repl_offset}"

        response = f"{replication}\r\n{master_replid}\r\n{master_repl_offset}"

        return self.builder.bulk_string(response)

    def handle_keys(self, args: List[str]) -> bytes:
        if not args:
            return self.builder.error("ERR wrong number of arguments for 'keys' command")

        pattern = args[0]
        if pattern != "*":
            return self.builder.array([])

        keys = self.store.get_all_keys()

        resp_keys = [self.builder.bulk_string(key) for key in keys]
        return self.builder.array(resp_keys)

    def _load_rdb(self):
        rdb_path = os.path.join(self.config["dir"], self.config["dbfilename"])
        try:
            rdb_parser = RdbParser(rdb_path)
            data = rdb_parser.parse()

            for key, (value, expiry) in data.items():
                self.store.set(key, value, expiry)
        except FileNotFoundError:
            # 파일이 없으면 빈 데이터베이스로 처리
            print(f"RDB file not found: {rdb_path}")
        except Exception as e:
            print(f"Error loading RDB file: {e}")


async def main() -> None:
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Redis Server')
    parser.add_argument('--dir', default='/tmp', help='Directory for RDB file')
    parser.add_argument('--dbfilename', default='dump.rdb', help='RDB filename')
    parser.add_argument('--port', default=6379, type=int, help='Redis server port')
    parser.add_argument('--replicaof', help='Redis server replicaof')
    args = parser.parse_args()

    print(f"Using dir: {args.dir}, dbfilename: {args.dbfilename}, port: {args.port}, replicaof: {args.replicaof}")

    server = RedisServer(dir_path=args.dir, dbfilename=args.dbfilename, replicaof=args.replicaof, port=args.port)
    await server.async_init()
    redis_server = await asyncio.start_server(
        server.handle_client, "localhost", args.port, reuse_port=True
    )

    async with redis_server:
        await redis_server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
