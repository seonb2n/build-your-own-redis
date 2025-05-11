import datetime
from typing import Dict, List, Optional, Tuple, Union

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

        return entry_id

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
