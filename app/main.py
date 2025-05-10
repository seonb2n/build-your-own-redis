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
    REPLCONF = "REPLCONF"
    PSYNC = "PSYNC"
    WAIT = "WAIT"

    WRITE_COMMANDS = {"SET", "DEL"}


class RespBuilder:
    @staticmethod
    def simple_string(value: str) -> bytes:
        return f"{RESP_SIMPLE_STRING_PREFIX.decode()}{value}{CRLF.decode()}".encode()

    @staticmethod
    def integer(value: int) -> bytes:
        return f":{value}{CRLF.decode()}".encode()

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
        self.replicas: List[asyncio.StreamWriter] = []
        self.replica_offsets = {}
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
            current_task = asyncio.current_task()
            current_task.writer = writer

            while True:
                data = await reader.read(1024)
                if not data:
                    break

                command, args = self.parser.parse(data)
                response = await self.handle_command(command, args, writer, from_master=False)

                if command == Commands.PSYNC or not writer in self.replicas:
                    writer.write(response)
                    await writer.drain()
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    def parse_command_from_buffer(self, buffer: bytes) -> Tuple[Optional[str], List[str], bytes, int]:
        try:
            # Find the first complete command
            if not buffer.startswith(RESP_ARRAY_PREFIX):
                return None, [], buffer

            # Find the end of the array prefix line
            array_end = buffer.find(CRLF)
            if array_end == -1:
                return None, [], buffer

            num_elements = int(buffer[1:array_end].decode())
            if num_elements < 1:
                return None, [], buffer[array_end + 2:]

            # Parse the command and arguments
            pos = array_end + 2
            args = []
            for _ in range(num_elements):
                if pos >= len(buffer):
                    return None, [], buffer
                if not buffer[pos:pos + 1] == RESP_BULK_STRING_PREFIX:
                    return None, [], buffer
                bulk_end = buffer.find(CRLF, pos)
                if bulk_end == -1:
                    return None, [], buffer
                length = int(buffer[pos + 1:bulk_end].decode())
                pos = bulk_end + 2
                if pos + length + 2 > len(buffer):
                    return None, [], buffer
                value = buffer[pos:pos + length].decode()
                args.append(value)
                pos += length + 2  # Skip value and trailing CRLF

            command = args[0].upper() if args else None

            return command, args[1:], buffer[pos:], pos
        except Exception:
            return None, [], buffer, 0

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

            # Step 5: Read FULLRESYNC response and RDB file
            fullresync_line = await reader.readline()
            print(f"Received from master: {fullresync_line}")

            if fullresync_line.startswith(b"+FULLRESYNC"):
                rdb_header = await reader.readline()
                if rdb_header.startswith(b"$"):
                    try:
                        rdb_length = int(rdb_header[1:-2].decode())

                        if rdb_length > 0:
                            rdb_content = await reader.readexactly(rdb_length)
                            print(f"Sync Completed: {rdb_content}")
                        elif rdb_length == 0:
                            print("Received empty RDB file (length 0).")
                        else:
                            print(f"Warning: Received unexpected RDB length: {rdb_length}")

                    except ValueError as e:
                        print(f"Error parsing RDB length: {e}, Header: {rdb_header}")
                    except Exception as e:
                        print(f"Error processing RDB file: {e}")

                else:
                    print(f"Error: Expected RDB bulk string starting with '$', but got: {rdb_header}")
                    writer.close()
                    await writer.wait_closed()
                    return

            else:
                print(f"Error: Expected FULLRESYNC, but got: {fullresync_line}")
                writer.close()
                await writer.wait_closed()
                return


            # Step 6: Continuously read propagated commands
            buffer = b""
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                buffer += data
                while buffer:
                    command, args, new_buffer, byte_size = self.parse_command_from_buffer(buffer)
                    if command is None:
                        break
                    buffer = new_buffer
                    if command in Commands.WRITE_COMMANDS:
                        # For write commands, we should update our state but not send a response
                        # We need to apply the command to our local store
                        if command == Commands.SET and len(args) >= 2:
                            key, value = args[0], args[1]
                            expiry = None
                            if len(args) > 2 and args[2].upper() == "PX" and len(args) > 3:
                                try:
                                    expiry_ms = int(args[3])
                                    expiry = datetime.datetime.now() + datetime.timedelta(milliseconds=expiry_ms)
                                except (ValueError, IndexError):
                                    pass
                            self.store.set(key, value, expiry)
                    elif command == Commands.REPLCONF and args and args[0].upper() == 'GETACK':
                        # Send ACK response back to master
                        ack_response = self.builder.array([
                            self.builder.bulk_string("REPLCONF"),
                            self.builder.bulk_string("ACK"),
                            self.builder.bulk_string(str(self.master_repl_offset))
                        ])
                        writer.write(ack_response)
                        await writer.drain()
                    self.master_repl_offset += byte_size

        except Exception as e:
            print(f"Failed to connect to master: {e}")

    async def handle_command(self, command: Optional[str], args: List[str], writer = None, from_master = False) -> bytes:
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
            Commands.REPLCONF: lambda args: self.handle_replconf(args, writer),
            Commands.PSYNC: lambda args: self.handle_psync(args, writer = writer),
            Commands.WAIT: self.handle_wait,
        }

        handler = handlers.get(command)

        if handler:
            if command == Commands.WAIT:
                response = await handler(args)
            else:
                response = handler(args)
                
            if command in Commands.WRITE_COMMANDS and not from_master:
                asyncio.create_task(self.propagate_command(command, args))

            return response

        return self.builder.error("ERR unknown command or invalid arguments")

    def handle_ping(self, args: List[str]) -> bytes:
        return self.builder.simple_string("PONG")

    async def handle_wait(self, args: List[str]) -> bytes:
        if len(args) < 2:
            return self.builder.error("ERR wrong number of arguments for 'wait' command")

        try:
            num_replicas = int(args[0])
            timeout_ms = int(args[1])
        except ValueError:
            return self.builder.error("ERR value is not an integer or out of range")

        current_offset = self.master_repl_offset
        acknowledged_replicas = 0

        # Send REPLCONF GETACK to all replicas
        getack_command = self.builder.array([
            self.builder.bulk_string("REPLCONF"),
            self.builder.bulk_string("GETACK"),
            self.builder.bulk_string("*")
        ])

        for replica in list(self.replicas):
            try:
                replica.write(getack_command)
                await replica.drain()
            except Exception as e:
                print(f"Error sending GETACK to replica: {e}")
                if replica in self.replicas:
                    self.replicas.remove(replica)
                    replica_id = id(replica)
                    if replica_id in self.replica_offsets:
                        del self.replica_offsets[replica_id]

        # Check initial acknowledgments
        for replica_id, offset in self.replica_offsets.items():
            if offset >= current_offset:
                acknowledged_replicas += 1

        if acknowledged_replicas >= num_replicas:
            return self.builder.integer(acknowledged_replicas)

        start_time = asyncio.get_event_loop().time()
        timeout = timeout_ms / 1000.0

        # Wait for acknowledgments or timeout
        while True:
            acknowledged_replicas = 0
            for replica_id, offset in list(self.replica_offsets.items()):
                if offset >= current_offset:
                    acknowledged_replicas += 1

            if acknowledged_replicas >= num_replicas:
                return self.builder.integer(acknowledged_replicas)

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                return self.builder.integer(acknowledged_replicas)

            await asyncio.sleep(0.01)


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

    def handle_replconf(self, args: List[str], writer = None) -> bytes:
        subcommand = args[0].upper()
        if subcommand == "GETACK":
            return self.builder.array([
                self.builder.bulk_string("REPLCONF"),
                self.builder.bulk_string("ACK"),
                self.builder.bulk_string(str(self.master_repl_offset))
            ])
        elif subcommand == "ACK" and len(args) > 1 and writer:
            try:
                replica_id = id(writer)
                ack_offset = int(args[1])
                self.replica_offsets[replica_id] = ack_offset
                print(f"Received ACK from replica {replica_id} with offset {ack_offset}")
            except (ValueError, AttributeError):
                pass
        return self.builder.simple_string("OK")

    def handle_psync(self, args: List[str], writer: asyncio.StreamWriter) -> bytes:
        # Step 1: Construct FULLRESYNC response
        response = f"FULLRESYNC {self.config['master_replid']} {self.master_repl_offset}"
        fullresync_response = self.builder.simple_string(response)

        # Step 2: Construct empty RDB file
        empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        empty_rdb_bytes = bytes.fromhex(empty_rdb_hex)
        rdb_response = f"${len(empty_rdb_bytes)}\r\n".encode() + empty_rdb_bytes

        # Add this replica to our list and initialize its offset
        if writer not in self.replicas:
            self.replicas.append(writer)
            replica_id = id(writer)
            self.replica_offsets[replica_id] = 0
            print(f"New replica connected: {replica_id}")

        return fullresync_response + rdb_response

    def _load_rdb_from_bytes(self, rdb_content: bytes) -> None:
        pass

    async def propagate_command(self, command: str, args: List[str]) -> None:
        if not self.replicas:
            return

        command_items = [self.builder.bulk_string(command)]
        for arg in args:
            command_items.append(self.builder.bulk_string(arg))
        command_array = self.builder.array(command_items)

        command_size = len(command_array)
        self.master_repl_offset += command_size

        for replica in list(self.replicas):
            try:
                replica.write(command_array)
                await replica.drain()
            except Exception as e:
                print(f"Error writing to replica: {e}")
                if replica in self.replicas:
                    self.replicas.remove(replica)
                    replica_id = id(replica)
                    if replica_id in self.replica_offsets:
                        del self.replica_offsets[replica_id]


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
    redis_server = await asyncio.start_server(
        server.handle_client, "localhost", args.port, reuse_port=True
    )

    await server.async_init()

    async with redis_server:
        await redis_server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
