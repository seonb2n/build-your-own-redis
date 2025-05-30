from typing import List, Optional, Tuple

CRLF = b"\r\n"
RESP_ARRAY_PREFIX = b"*"
RESP_BULK_STRING_PREFIX = b"$"
RESP_SIMPLE_STRING_PREFIX = b"+"
RESP_ERROR_PREFIX = b"-"

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

    @staticmethod
    def parse_command_from_buffer(buffer: bytes) -> Tuple[Optional[str], List[str], bytes, int]:
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

    @staticmethod
    def nested_array(items: List[List]) -> bytes:
        result = f"{RESP_ARRAY_PREFIX.decode()}{len(items)}{CRLF.decode()}".encode()
        for item in items:
            # 각 항목은 [id, [field1, value1, ...]] 형식
            entry_id, fields = item

            result += f"{RESP_ARRAY_PREFIX.decode()}2{CRLF.decode()}".encode()

            result += f"{RESP_BULK_STRING_PREFIX.decode()}{len(entry_id)}{CRLF.decode()}{entry_id}{CRLF.decode()}".encode()

            result += f"{RESP_ARRAY_PREFIX.decode()}{len(fields)}{CRLF.decode()}".encode()

            for field_item in fields:
                field_str = str(field_item)  # 값이 문자열이 아닐 수 있으므로 변환
                result += f"{RESP_BULK_STRING_PREFIX.decode()}{len(field_str)}{CRLF.decode()}{field_str}{CRLF.decode()}".encode()

        return result

    @staticmethod
    def xread_response(streams_data):
        """
        XREAD 응답 형식에 맞게 데이터 구성
        streams_data: [["key1", [["id1", ["field1", "value1", ...]], ...]], ...]
        """
        result = f"{RESP_ARRAY_PREFIX.decode()}{len(streams_data)}{CRLF.decode()}".encode()

        for stream_entry in streams_data:
            key, entries = stream_entry

            # 스트림 키와 항목 배열을 포함하는 배열
            result += f"{RESP_ARRAY_PREFIX.decode()}2{CRLF.decode()}".encode()

            # 스트림 키
            key_bytes = key.encode()
            result += f"{RESP_BULK_STRING_PREFIX.decode()}{len(key_bytes)}{CRLF.decode()}".encode() + key_bytes + CRLF

            # 항목 배열
            result += f"{RESP_ARRAY_PREFIX.decode()}{len(entries)}{CRLF.decode()}".encode()

            for entry in entries:
                entry_id, fields_values = entry

                # 항목 ID와 필드/값 배열을 포함하는 배열
                result += f"{RESP_ARRAY_PREFIX.decode()}2{CRLF.decode()}".encode()

                # 항목 ID
                id_bytes = entry_id.encode()
                result += f"{RESP_BULK_STRING_PREFIX.decode()}{len(id_bytes)}{CRLF.decode()}".encode() + id_bytes + CRLF

                # 필드/값 배열
                result += f"{RESP_ARRAY_PREFIX.decode()}{len(fields_values)}{CRLF.decode()}".encode()

                for field_or_value in fields_values:
                    field_value_bytes = str(field_or_value).encode()
                    result += f"{RESP_BULK_STRING_PREFIX.decode()}{len(field_value_bytes)}{CRLF.decode()}".encode() + field_value_bytes + CRLF

        return result