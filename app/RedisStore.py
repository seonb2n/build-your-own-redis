import datetime
import time
from typing import Dict, List, Optional, Tuple, Union
import re

class RedisStore:
    def __init__(self):
        self.data: Dict[str, Tuple[str, Union[str, List[Dict]], Union[int, datetime.datetime]]] = {}

    def set(self, key: str, value: str, expiry: Optional[datetime.datetime] = None) -> None:
        self.data[key] = ("string", value, expiry if expiry else -1)

    def get(self, key: str) -> Optional[Tuple[str, Union[str, List[Dict]]]]:
        """Get the type and value of a key (string or stream)."""
        if key not in self.data:
            return None

        data_type, value, expiry = self.data[key]

        # 만료 시간 확인
        if expiry != -1 and datetime.datetime.now() >= expiry:
            del self.data[key]
            return None

        return data_type, value

    def incr(self, key: str) -> int:
        if key not in self.data:
            self.data[key] = ("string", "1", -1)
            return 1
        _, value = self.get(key)
        try:
            int_value = int(value)
        except ValueError:
            raise ValueError("ERR value is not an integer or out of range")
        incr_value = str(int_value + 1)
        self.set(key, incr_value)
        return int(incr_value)

    def type(self, key: str) -> str:
        if key not in self.data:
            return "none"
        data_type, _, expiry = self.data[key]
        if expiry != -1 and datetime.datetime.now() >= expiry:
            del self.data[key]
            return "none"
        return data_type

    def xadd(self, key: str, entry_id: str, fields: List[str]) -> str:
        if len(fields) % 2 != 0:
            raise ValueError("Fields must be provided as key-value pairs")

        stream = None
        if key in self.data and self.data[key][0] == 'stream':
            stream = self.data[key][1]

        if entry_id == "*":
            current_time_ms = int(time.time() * 1000)
            seq = 0
            if stream:
                for entry in stream:
                    entry_time, entry_seq = map(int, entry["id"].split('-'))
                    if entry_time == current_time_ms:
                        seq = max(seq, entry_seq + 1)
            entry_id = f"{current_time_ms}-{seq}"

        elif '-*' in entry_id:
            time_part = entry_id.split('-')[0]
            time_part_int = int(time_part)
            max_seq = -1
            if stream:
                for entry in stream:
                    entry_time, entry_seq = map(int, entry["id"].split('-'))
                    if entry_time == time_part_int and entry_seq > max_seq:
                        max_seq = entry_seq

            if max_seq == -1:
                if time_part_int == 0:
                    seq = 1  # time_part가 0일 경우 시퀀스는 1로 시작
                else:
                    seq = 0  # 기본 시퀀스는 0
            else:
                seq = max_seq + 1  # 마지막 시퀀스 번호 + 1
            # 새 entry_id 생성
            entry_id = f"{time_part}-{seq}"
        else:
            self.validate_entry_id(entry_id, stream)


        entry = {
            "id": entry_id,
            "fields": {fields[i]: fields[i + 1] for i in range(0, len(fields), 2)}
        }

        if key not in self.data:
            # Create a new stream
            self.data[key] = ("stream", [entry], -1)
        else:
            data_type, value, expiry = self.data[key]
            if data_type != "stream":
                raise ValueError(f"Key {key} is not a stream")

            # Check expiry
            if expiry != -1 and datetime.datetime.now() >= expiry:
                del self.data[key]
                self.data[key] = ("stream", [entry], -1)
            else:
                # Append to existing stream
                value.append(entry)
                self.data[key] = ("stream", value, expiry)

        _, stream, expiry = self.data[key]

        return entry_id

    def xrange(self, key: str, entry_start: str, entry_end: str) -> List[List]:
        if key in self.data and self.data[key][0] != 'stream':
            return []

        _, stream, expiry = self.data[key]

        # 만료 시간 확인
        if expiry != -1 and datetime.datetime.now() >= expiry:
            del self.data[key]
            return []

        if entry_start == '-':
            entry_start = '0-0'

        if '-' not in entry_start:
            entry_start = f"{entry_start}-0"

        if entry_end == '+':
            entry_end = "18446744073709551615-18446744073709551615"

        if '-' not in entry_end:
            entry_end = f"{entry_end}-18446744073709551615"

        start_time, start_seq = map(int, entry_start.split('-'))
        end_time, end_seq = map(int, entry_end.split('-'))

        # 시작 ID 부터 종료 ID 까지 필터링
        result = []
        for entry in stream:
            entry_id = entry["id"]

            entry_time, entry_seq = map(int, entry_id.split('-'))
            if (entry_time > start_time or (entry_time == start_time and entry_seq >= start_seq)) \
                and (entry_time < end_time or (entry_time == end_time and entry_seq <= end_seq)):
                fields_list = []
                for field_name, field_value in entry["fields"].items():
                    fields_list.append(field_name)
                    fields_list.append(field_value)

                result.append([entry_id, fields_list])
        return result

    def xread(self, key: str, start_entry: str) -> List[List]:
        if key in self.data and self.data[key][0] != 'stream':
            return []

        _, stream, expiry = self.data[key]

        result = []
        start_time, start_seq = map(int, start_entry.split('-'))
        for entry in stream:
            entry_id = entry["id"]
            entry_time, entry_seq = map(int, entry_id.split('-'))
            if entry_time > start_time or (entry_time == start_time and entry_seq > start_seq):
                fields_list = []
                for field_name, field_value in entry["fields"].items():
                    fields_list.append(field_name)
                    fields_list.append(field_value)

                result.append([entry_id, fields_list])
        if not result:
            return []
        return [[key, result]]


    def get_all_keys(self) -> List[str]:
        """만료되지 않은 모든 키 목록 반환"""
        now = datetime.datetime.now()
        result = []

        for key, (data_type, value, expiry) in list(self.data.items()):
            # 만료 시간 확인
            if expiry == -1 or now < expiry:
                result.append(key)
            else:
                # 만료된 키 삭제
                del self.data[key]

        return result

    def get_last_id(self, key: str) -> Optional[str]:
        """스트림의 마지막 ID 반환

        Args:
            key: 스트림 키

        Returns:
            마지막 엔트리의 ID 또는 스트림이 존재하지 않거나 비어있으면 None
        """
        if key not in self.data:
            return None

        data_type, value, expiry = self.data[key]

        # 키가 스트림 타입이 아니면 None 반환
        if data_type != "stream":
            return None

        # 만료 시간 확인
        if expiry != -1 and datetime.datetime.now() >= expiry:
            del self.data[key]
            return None

        # 스트림이 비어있는지 확인
        if not value or len(value) == 0:
            return None

        # 마지막 엔트리의 ID 반환
        return value[-1]["id"]

    def validate_entry_id(self, entry_id: str, stream: Optional[List[Dict]]) -> None:
        if not re.match(r'^\d+-\d+$', entry_id):
            raise ValueError("ERR Invalid entry ID format")

        try:
            millis, seq = map(int, entry_id.split('-'))
        except ValueError:
            raise ValueError("ERR Invalid entry ID format")

        if millis == 0 and seq == 0:
            raise ValueError("ERR The ID specified in XADD must be greater than 0-0")

        if stream and stream[-1]["id"]:
            last_id = stream[-1]["id"]
            last_millis, last_seq = map(int, last_id.split('-'))

            # ID must be greater than the last entry's ID
            if millis < last_millis or (millis == last_millis and seq <= last_seq):
                raise ValueError(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                )