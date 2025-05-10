from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

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
