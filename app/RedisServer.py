import os.path
import datetime
import time
from typing import Dict, List, Optional, Tuple, Any, Set, Callable
import asyncio
from enum import Enum, auto

from app.RdbParse import RdbParser
from app.RedisStore import RedisStore
from app.RespUtils import RespBuilder, RespParser


class CommandType(Enum):
    """Command type classification for Redis commands"""
    READ = auto()
    WRITE = auto()
    ADMIN = auto()
    REPLICATION = auto()


class Commands:
    """Registry of supported Redis commands with their types"""
    # Command names
    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"
    CONFIG = "CONFIG"
    KEYS = "KEYS"
    TYPE = "TYPE"
    INFO = "INFO"
    REPLCONF = "REPLCONF"
    PSYNC = "PSYNC"
    WAIT = "WAIT"
    XADD = "XADD"
    XRANGE = "XRANGE"
    XREAD = "XREAD"
    INCR = "INCR"
    MULTI = "MULTI"
    EXEC = "EXEC"

    # Command classifications
    WRITE_COMMANDS = {SET, "DEL"}
    
    # Command type mapping
    COMMAND_TYPES = {
        PING: CommandType.READ,
        ECHO: CommandType.READ,
        SET: CommandType.WRITE,
        GET: CommandType.READ,
        XADD: CommandType.WRITE,
        XRANGE: CommandType.READ,
        XREAD: CommandType.READ,
        CONFIG: CommandType.ADMIN,
        KEYS: CommandType.READ,
        TYPE: CommandType.READ,
        INFO: CommandType.ADMIN,
        REPLCONF: CommandType.REPLICATION,
        PSYNC: CommandType.REPLICATION,
        WAIT: CommandType.REPLICATION,
        INCR: CommandType.WRITE,
        MULTI: CommandType.WRITE,
        EXEC: CommandType.WRITE,
    }
    
    @classmethod
    def is_write_command(cls, command: str) -> bool:
        return command in cls.WRITE_COMMANDS


class ReplicationManager:
    """Manages replication logic for the Redis server"""
    
    def __init__(self, server):
        self._server = server
        self._replicas: List[asyncio.StreamWriter] = []
        self._replica_offsets: Dict[int, int] = {}
        self._master_repl_offset: int = 0
        self._master_host: Optional[str] = None
        self._master_port: Optional[int] = None
    
    @property
    def master_repl_offset(self) -> int:
        """Get the current master replication offset"""
        return self._master_repl_offset
    
    @property
    def replicas(self) -> List[asyncio.StreamWriter]:
        """Get the list of connected replicas"""
        return self._replicas
    
    @property
    def replica_offsets(self) -> Dict[int, int]:
        """Get the dictionary of replica offsets"""
        return self._replica_offsets
    
    def add_replica(self, writer: asyncio.StreamWriter) -> None:
        """Add a new replica connection"""
        if writer not in self._replicas:
            self._replicas.append(writer)
            replica_id = id(writer)
            self._replica_offsets[replica_id] = 0
            print(f"New replica connected: {replica_id}")
    
    def remove_replica(self, writer: asyncio.StreamWriter) -> None:
        """Remove a replica connection"""
        if writer in self._replicas:
            self._replicas.remove(writer)
            replica_id = id(writer)
            if replica_id in self._replica_offsets:
                del self._replica_offsets[replica_id]
    
    def increment_offset(self, size: int) -> None:
        """Increment the master replication offset"""
        self._master_repl_offset += size
    
    def set_master_info(self, host: str, port: int) -> None:
        """Set master server information"""
        self._master_host = host
        self._master_port = port
    
    @property
    def has_master(self) -> bool:
        """Check if this server has a master configured"""
        return self._master_host is not None and self._master_port is not None
    
    async def propagate_command(self, command: str, args: List[str], builder: RespBuilder) -> None:
        """Propagate a command to all replicas"""
        if not self._replicas:
            return

        command_items = [builder.bulk_string(command)]
        for arg in args:
            command_items.append(builder.bulk_string(arg))
        command_array = builder.array(command_items)

        command_size = len(command_array)
        self._master_repl_offset += command_size

        for replica in list(self._replicas):
            try:
                replica.write(command_array)
                await replica.drain()
            except Exception as e:
                print(f"Error writing to replica: {e}")
                self.remove_replica(replica)
    
    async def send_getack_to_replicas(self, builder: RespBuilder) -> None:
        """Send GETACK command to all replicas"""
        getack_command = builder.array([
            builder.bulk_string("REPLCONF"),
            builder.bulk_string("GETACK"),
            builder.bulk_string("*")
        ])

        for replica in list(self._replicas):
            try:
                replica.write(getack_command)
                await replica.drain()
            except Exception as e:
                print(f"Error sending GETACK to replica: {e}")
                self.remove_replica(replica)
    
    def count_acknowledged_replicas(self, target_offset: int) -> int:
        """Count replicas that have acknowledged up to the target offset"""
        acknowledged = 0
        for offset in self._replica_offsets.values():
            if offset >= target_offset:
                acknowledged += 1
        return acknowledged


class RedisServer:
    """Redis server implementation with proper encapsulation and separation of concerns"""
    
    def __init__(self, dir_path="/tmp", dbfilename="dump.rdb", replicaof: Optional[str] = None, port: int = 6379):
        # Initialize components with proper encapsulation
        self._store = RedisStore()
        self._parser = RespParser()
        self._builder = RespBuilder()
        self._replication = ReplicationManager(self)
        
        # Server configuration
        self._config = {
            "dir": dir_path,
            "dbfilename": dbfilename,
            "replicaof": "slave" if replicaof else "master",
            "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        }
        
        # Server properties
        self._port = port
        self._replicaof = replicaof

        self._client_transactions = {}
        
        # Load initial data
        self._load_rdb()
        
        # Command handlers mapping
        self._command_handlers = self._register_command_handlers()
    
    def _register_command_handlers(self) -> Dict[str, Callable]:
        """Register all command handlers with their respective methods"""
        return {
            Commands.PING: self._handle_ping,
            Commands.ECHO: self._handle_echo,
            Commands.SET: self._handle_set,
            Commands.GET: self._handle_get,
            Commands.XADD: self._handle_xadd,
            Commands.XRANGE: self._handle_xrange,
            Commands.XREAD: self._handle_xread,
            Commands.TYPE: self._handle_type,
            Commands.CONFIG: self._handle_config,
            Commands.KEYS: self._handle_keys,
            Commands.INFO: self._handle_info,
            Commands.REPLCONF: self._handle_replconf,
            Commands.PSYNC: self._handle_psync,
            Commands.WAIT: self._handle_wait,
            Commands.INCR: self._handle_incr,
            Commands.MULTI: self._handle_multi,
            Commands.EXEC: self._handle_exec,
        }

    async def async_init(self) -> None:
        """Initialize async components of the server"""
        if self._replicaof:
            try:
                host, port_str = self._replicaof.split()
                port = int(port_str)
                self._replication.set_master_info(host, port)
                await self._connect_to_master()
            except ValueError:
                raise ValueError("Invalid replicaof format. Expected 'host port'.")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle a client connection"""
        try:
            current_task = asyncio.current_task()
            current_task.writer = writer

            while True:
                data = await reader.read(1024)
                if not data:
                    break

                command, args = self._parser.parse(data)
                response = await self._process_command(command, args, writer, from_master=False)

                if command == Commands.PSYNC or writer not in self._replication.replicas:
                    writer.write(response)
                    await writer.drain()
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def _process_command(self, command: Optional[str], args: List[str], writer=None, from_master=False) -> bytes:
        """Process a command and return the response"""
        if command is None:
            return self._builder.error("ERR protocol error")

        client_id = id(writer) if writer else None

        if command == Commands.MULTI:
            return self._handle_multi(client_id)

        if command == Commands.EXEC:
            return await self._handle_exec(client_id)

        # Check if client is in a transaction
        if client_id in self._client_transactions and self._client_transactions[client_id]['in_multi']:
            if command != "EXEC" and command != "DISCARD":
                self._queue_command(client_id, command, args)
                return self._builder.simple_string("QUEUED")

        handler = self._command_handlers.get(command)

        if handler:
            # Special case for REPLCONF and PSYNC which need writer
            if command == Commands.REPLCONF:
                response = handler(args, writer)
            elif command == Commands.PSYNC:
                response = handler(args, writer)
            # Special case for WAIT which is async
            elif command == Commands.WAIT or command == Commands.XREAD:
                response = await handler(args)
            else:
                response = handler(args)
            
            # Propagate write commands to replicas
            if Commands.is_write_command(command) and not from_master:
                asyncio.create_task(self._replication.propagate_command(command, args, self._builder))

            return response

        return self._builder.error("ERR unknown command or invalid arguments")

    async def _connect_to_master(self) -> None:
        """Connect to a master Redis server for replication"""
        if not self._replication.has_master:
            return
            
        try:
            reader, writer = await asyncio.open_connection(
                self._replication._master_host, 
                self._replication._master_port
            )
            
            # Step 1: Handshake with master
            await self._perform_master_handshake(reader, writer)
            
            # Step 2: Process master's RDB file
            await self._process_master_rdb(reader, writer)
            
            # Step 3: Process ongoing commands from master
            await self._process_master_commands(reader, writer)
            
        except Exception as e:
            print(f"Failed to connect to master: {e}")
    
    async def _perform_master_handshake(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Perform initial handshake with master"""
        # Send PING
        ping_command = self._builder.array([self._builder.bulk_string("PING")])
        writer.write(ping_command)
        await writer.drain()
        await reader.read(1024)  # Read response
        
        # Send REPLCONF listening-port
        replconf_port = self._builder.array([
            self._builder.bulk_string("REPLCONF"),
            self._builder.bulk_string("listening-port"),
            self._builder.bulk_string(str(self._port))
        ])
        writer.write(replconf_port)
        await writer.drain()
        await reader.read(1024)  # Read response
        
        # Send REPLCONF capa psync2
        replconf_capa = self._builder.array([
            self._builder.bulk_string("REPLCONF"),
            self._builder.bulk_string("capa"),
            self._builder.bulk_string("psync2")
        ])
        writer.write(replconf_capa)
        await writer.drain()
        await reader.read(1024)  # Read response
        
        # Send PSYNC
        psync = self._builder.array([
            self._builder.bulk_string("psync"),
            self._builder.bulk_string("?"),
            self._builder.bulk_string("-1")
        ])
        writer.write(psync)
        await writer.drain()
    
    async def _process_master_rdb(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bool:
        """Process the RDB file sent by master during initial sync"""
        # Read FULLRESYNC response
        fullresync_line = await reader.readline()
        print(f"Received from master: {fullresync_line}")
        
        if not fullresync_line.startswith(b"+FULLRESYNC"):
            print(f"Error: Expected FULLRESYNC, but got: {fullresync_line}")
            writer.close()
            await writer.wait_closed()
            return False
            
        # Read RDB file header
        rdb_header = await reader.readline()
        if not rdb_header.startswith(b"$"):
            print(f"Error: Expected RDB bulk string starting with '$', but got: {rdb_header}")
            writer.close()
            await writer.wait_closed()
            return False
            
        try:
            rdb_length = int(rdb_header[1:-2].decode())
            
            if rdb_length > 0:
                rdb_content = await reader.readexactly(rdb_length)
                print(f"Sync Completed: {rdb_content}")
                # Process RDB content here if needed
            elif rdb_length == 0:
                print("Received empty RDB file (length 0).")
            else:
                print(f"Warning: Received unexpected RDB length: {rdb_length}")
                
            return True
                
        except ValueError as e:
            print(f"Error parsing RDB length: {e}, Header: {rdb_header}")
        except Exception as e:
            print(f"Error processing RDB file: {e}")
            
        return False
    
    async def _process_master_commands(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Process ongoing commands from master after initial sync"""
        buffer = b""
        while True:
            data = await reader.read(1024)
            if not data:
                break
                
            buffer += data
            while buffer:
                command, args, new_buffer, byte_size = self._parser.parse_command_from_buffer(buffer)
                if command is None:
                    break
                    
                buffer = new_buffer
                
                # Process the command based on its type
                if command in Commands.WRITE_COMMANDS:
                    await self._apply_write_command(command, args)
                elif command == Commands.REPLCONF and args and args[0].upper() == 'GETACK':
                    await self._send_ack_to_master(writer)
                    
                self._replication.increment_offset(byte_size)
    
    async def _apply_write_command(self, command: str, args: List[str]) -> None:
        """Apply a write command received from master"""
        if command == Commands.SET and len(args) >= 2:
            key, value = args[0], args[1]
            expiry = None
            
            if len(args) > 2 and args[2].upper() == "PX" and len(args) > 3:
                try:
                    expiry_ms = int(args[3])
                    expiry = datetime.datetime.now() + datetime.timedelta(milliseconds=expiry_ms)
                except (ValueError, IndexError):
                    pass
                    
            self._store.set(key, value, expiry)
    
    async def _send_ack_to_master(self, writer: asyncio.StreamWriter) -> None:
        """Send ACK response back to master"""
        ack_response = self._builder.array([
            self._builder.bulk_string("REPLCONF"),
            self._builder.bulk_string("ACK"),
            self._builder.bulk_string(str(self._replication.master_repl_offset))
        ])
        writer.write(ack_response)
        await writer.drain()

    # Command handlers
    def _handle_ping(self, args: List[str]) -> bytes:
        """Handle PING command"""
        return self._builder.simple_string("PONG")

    def _handle_echo(self, args: List[str]) -> bytes:
        """Handle ECHO command"""
        if not args:
            return self._builder.error("ERR wrong number of arguments for 'echo' command")
        return self._builder.simple_string(args[0])

    def _handle_set(self, args: List[str]) -> bytes:
        """Handle SET command"""
        if len(args) < 2:
            return self._builder.error("ERR wrong number of arguments for 'set' command")

        key, value = args[0], args[1]
        expiry = None

        # Handle PX option
        if len(args) > 2 and args[2].lower() == 'px' and len(args) > 3:
            try:
                milliseconds = int(args[3])
                expiry = datetime.datetime.now() + datetime.timedelta(milliseconds=milliseconds)
            except ValueError:
                return self._builder.error("ERR value is not an integer or out of range")

        self._store.set(key, value, expiry)
        return self._builder.simple_string("OK")

    def _handle_get(self, args: List[str]) -> bytes:
        """Handle GET command"""
        if len(args) != 1:
            return self._builder.error("ERR wrong number of arguments for 'get' command")

        result = self._store.get(args[0])
        if result is None:
            return self._builder.null()

        data_type, value = result
        return self._builder.bulk_string(value)

    def _handle_incr(self, args: List[str]) -> bytes:
        key = args[0]
        try:
            result = self._store.incr(key)
        except ValueError as e:
            error_message = str(e)
            return self._builder.error(error_message)
        return self._builder.integer(result)

    def _handle_xadd(self, args: List[str]) -> bytes:
        """Handle XADD command"""
        key = args[0]
        entry_id = args[1]
        values = args[2:]
        try:
            result = self._store.xadd(key, entry_id, values)
        except ValueError as e:
            error_message = str(e)
            return self._builder.error(error_message)

        return self._builder.bulk_string(result)

    def _handle_xrange(self, args: List[str]) -> bytes:
        """Handle XRANGE command"""
        key = args[0]
        entry_start = args[1]
        entry_end = args[2]
        try:
            result = self._store.xrange(key, entry_start, entry_end)
        except ValueError as e:
            error_message = str(e)
            return self._builder.error(error_message)
        return self._builder.nested_array(result)

    def _handle_multi(self, client_id: Optional[int]) -> bytes:
        """Handle MULTI command"""
        if client_id is None:
            return self._builder.simple_string("OK")

        # Initialize transaction state for this client
        self._client_transactions[client_id] = {
            'in_multi': True,
            'queue': []
        }

        return self._builder.simple_string("OK")

    def _queue_command(self, client_id: int, command: str, args: List[str]) -> None:
        """Queue a command for later execution"""
        if client_id in self._client_transactions:
            self._client_transactions[client_id]['queue'].append((command, args))

    async def _handle_exec(self, client_id: int) -> bytes:
        """Handle EXEC command - execute all queued commands in transaction"""
        if client_id not in self._client_transactions:
            return self._builder.error("ERR EXEC without MULTI")

        transaction = self._client_transactions[client_id]

        if not transaction.get('in_multi', False):
            return self._builder.error("ERR EXEC without MULTI")

        if not transaction['queue']:
            del self._client_transactions[client_id]
            return self._builder.array([])

        command_results = []

        # 큐에 있는 모든 명령어를 순서대로 실행
        for command, args in transaction['queue']:
            try:
                result = await self._process_command(command, args, from_master=False)
                command_results.append(result)
            except Exception as e:
                error_response = self._builder.error(f"ERR {str(e)}")
                command_results.append(error_response)

        # 트랜잭션 상태 정리
        del self._client_transactions[client_id]

        # 모든 결과를 배열로 반환
        return self._builder.array(command_results)

    async def _handle_xread(self, args: List[str]) -> bytes:
        """Handle XREAD command"""
        if "streams" not in args:
            return self._builder.error("ERR syntax error, STREAMS keyword required")

        streams_index = args.index("streams")
        options = {}
        i = 0
        while i < streams_index:
            if args[i].upper() == "BLOCK" and i + 1 < streams_index:
                try:
                    options["block"] = int(args[i + 1])
                    i += 2
                except ValueError:
                    return self._builder.error("ERR value is not an integer or out of range")
            else:
                i += 1  # 알 수 없는 옵션은 건너뜀

        keys_and_ids = args[streams_index + 1:]
        num_keys = len(keys_and_ids) // 2
        if len(keys_and_ids) % 2 != 0 or num_keys == 0:
            return self._builder.error("ERR wrong number of arguments for 'xread' command")

        keys = keys_and_ids[:num_keys]
        ids = keys_and_ids[num_keys:]

        # $ 식별자 처리 - 새로운 스트림 항목을 기다리기 위한 최신 ID 처리
        for i, id_str in enumerate(ids):
            if id_str == "$":
                # 해당 스트림의 마지막 ID 얻기
                last_id = self._store.get_last_id(keys[i])
                if last_id:
                    ids[i] = last_id
                else:
                    # 스트림이 비어있거나 존재하지 않는 경우 0-0 사용
                    ids[i] = "0-0"

        # 초기 결과 확인
        final_result = self._get_xread_results(keys, ids, options)
        if final_result:
            return self._builder.xread_response(final_result)

        # BLOCK 옵션이 있고 결과가 없는 경우 대기
        block_timeout = options.get("block")
        if block_timeout is None:
            # block 옵션이 없으면 바로 결과 반환
            return self._builder.null() if not final_result else self._builder.xread_response(final_result)

        # 블로킹 모드에서의 대기 처리
        start_time = time.time()

        # block이 0인 경우, 결과가 나올 때까지 계속 폴링
        if block_timeout == 0:
            while True:
                # 잠깐 대기 후 다시 확인
                await asyncio.sleep(0.1)  # 100ms마다 폴링
                final_result = self._get_xread_results(keys, ids, options)
                if final_result:
                    return self._builder.xread_response(final_result)
        else:
            # 지정된 시간동안 폴링
            end_time = start_time + (block_timeout / 1000)
            while time.time() < end_time:
                await asyncio.sleep(0.1)  # 100ms마다 폴링
                final_result = self._get_xread_results(keys, ids, options)
                if final_result:
                    return self._builder.xread_response(final_result)

            # 타임아웃에 도달하면 nil 반환
            return self._builder.null()

    def _get_xread_results(self, keys: List[str], ids: List[str], options: Dict) -> List:
        """XREAD 결과 가져오기"""
        final_result = []
        # 각 스트림에 대해 처리
        for i, key in enumerate(keys):
            try:
                # 각 키에 대한 결과 가져오기
                result = self._store.xread(key, ids[i])
                if result and len(result) > 0:  # 결과가 있는 경우에만 추가
                    final_result.extend(result)
            except ValueError as e:
                # 여기서는 오류 발생 시 해당 스트림만 건너뜀
                continue
        return final_result

    def _handle_type(self, args: List[str]) -> bytes:
        """Handle TYPE command"""
        if not args:
            return self._builder.error("ERR wrong number of arguments for 'type' command")

        data_type = self._store.type(args[0])
        return self._builder.simple_string(data_type)


    def _handle_config(self, args: List[str]) -> bytes:
        """Handle CONFIG command"""
        if len(args) < 2:
            return self._builder.error("ERR wrong number of arguments for 'config' command")

        subcommand = args[0].upper()
        if subcommand != "GET":
            return self._builder.error("ERR unknown subcommand for CONFIG")

        param_name = args[1]
        if param_name not in self._config:
            return self._builder.array([])

        return self._builder.array([
            self._builder.bulk_string(param_name),
            self._builder.bulk_string(self._config[param_name])
        ])

    def _handle_info(self, args: List[str]) -> bytes:
        """Handle INFO command"""
        replication = f"role:{self._config['replicaof']}"
        master_replid = f"master_replid:{self._config['master_replid']}"
        master_repl_offset = f"master_repl_offset:{self._replication.master_repl_offset}"

        response = f"{replication}\r\n{master_replid}\r\n{master_repl_offset}"

        return self._builder.bulk_string(response)

    def _handle_keys(self, args: List[str]) -> bytes:
        """Handle KEYS command"""
        if not args:
            return self._builder.error("ERR wrong number of arguments for 'keys' command")

        pattern = args[0]
        if pattern != "*":
            return self._builder.array([])

        keys = self._store.get_all_keys()

        resp_keys = [self._builder.bulk_string(key) for key in keys]
        return self._builder.array(resp_keys)

    def _handle_replconf(self, args: List[str], writer=None) -> bytes:
        """Handle REPLCONF command"""
        subcommand = args[0].upper()
        
        if subcommand == "GETACK":
            return self._builder.array([
                self._builder.bulk_string("REPLCONF"),
                self._builder.bulk_string("ACK"),
                self._builder.bulk_string(str(self._replication.master_repl_offset))
            ])
        elif subcommand == "ACK" and len(args) > 1 and writer:
            try:
                replica_id = id(writer)
                ack_offset = int(args[1])
                self._replication.replica_offsets[replica_id] = ack_offset
                print(f"Received ACK from replica {replica_id} with offset {ack_offset}")
            except (ValueError, AttributeError):
                pass
                
        return self._builder.simple_string("OK")

    def _handle_psync(self, args: List[str], writer: asyncio.StreamWriter) -> bytes:
        """Handle PSYNC command"""
        # Step 1: Construct FULLRESYNC response
        response = f"FULLRESYNC {self._config['master_replid']} {self._replication.master_repl_offset}"
        fullresync_response = self._builder.simple_string(response)

        # Step 2: Construct empty RDB file
        empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        empty_rdb_bytes = bytes.fromhex(empty_rdb_hex)
        rdb_response = f"${len(empty_rdb_bytes)}\r\n".encode() + empty_rdb_bytes

        # Add this replica to our list and initialize its offset
        self._replication.add_replica(writer)

        return fullresync_response + rdb_response

    async def _handle_wait(self, args: List[str]) -> bytes:
        """Handle WAIT command"""
        if len(args) < 2:
            return self._builder.error("ERR wrong number of arguments for 'wait' command")

        try:
            num_replicas = int(args[0])
            timeout_ms = int(args[1])
        except ValueError:
            return self._builder.error("ERR value is not an integer or out of range")

        current_offset = self._replication.master_repl_offset
        
        # Send GETACK to all replicas
        await self._replication.send_getack_to_replicas(self._builder)

        # Check initial acknowledgments
        acknowledged_replicas = self._replication.count_acknowledged_replicas(current_offset)
        
        if acknowledged_replicas >= num_replicas:
            return self._builder.integer(acknowledged_replicas)

        # Wait for acknowledgments or timeout
        start_time = asyncio.get_event_loop().time()
        timeout = timeout_ms / 1000.0
        
        while True:
            acknowledged_replicas = self._replication.count_acknowledged_replicas(current_offset)
            
            if acknowledged_replicas >= num_replicas:
                return self._builder.integer(acknowledged_replicas)

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                return self._builder.integer(acknowledged_replicas)

            await asyncio.sleep(0.01)

    def _load_rdb(self) -> None:
        """Load data from RDB file"""
        rdb_path = os.path.join(self._config["dir"], self._config["dbfilename"])
        try:
            rdb_parser = RdbParser(rdb_path)
            data = rdb_parser.parse()

            for key, (value, expiry) in data.items():
                self._store.set(key, value, expiry)
        except FileNotFoundError:
            print(f"RDB file not found: {rdb_path}")
        except Exception as e:
            print(f"Error loading RDB file: {e}")
