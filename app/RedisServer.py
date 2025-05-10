import os.path
import datetime
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
    INFO = "INFO"
    REPLCONF = "REPLCONF"
    PSYNC = "PSYNC"
    WAIT = "WAIT"

    # Command classifications
    WRITE_COMMANDS = {SET, "DEL"}
    
    # Command type mapping
    COMMAND_TYPES = {
        PING: CommandType.READ,
        ECHO: CommandType.READ,
        SET: CommandType.WRITE,
        GET: CommandType.READ,
        CONFIG: CommandType.ADMIN,
        KEYS: CommandType.READ,
        INFO: CommandType.ADMIN,
        REPLCONF: CommandType.REPLICATION,
        PSYNC: CommandType.REPLICATION,
        WAIT: CommandType.REPLICATION,
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
            Commands.CONFIG: self._handle_config,
            Commands.KEYS: self._handle_keys,
            Commands.INFO: self._handle_info,
            Commands.REPLCONF: self._handle_replconf,
            Commands.PSYNC: self._handle_psync,
            Commands.WAIT: self._handle_wait,
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

        handler = self._command_handlers.get(command)
        
        if handler:
            # Special case for REPLCONF and PSYNC which need writer
            if command == Commands.REPLCONF:
                response = handler(args, writer)
            elif command == Commands.PSYNC:
                response = handler(args, writer)
            # Special case for WAIT which is async
            elif command == Commands.WAIT:
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
        if not args:
            return self._builder.error("ERR wrong number of arguments for 'get' command")

        value = self._store.get(args[0])
        if value is None:
            return self._builder.null()

        return self._builder.simple_string(value)

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
