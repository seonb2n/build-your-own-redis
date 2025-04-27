import datetime


class RdbParser:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = {}

    def parse(self):
        try:
            with open(self.file_path, 'rb') as f:
                # 헤더 확인 (REDIS0011)
                header = f.read(9)
                if header != b'REDIS0011':
                    raise ValueError(f"Invalid RDB header: {header}")

                # 메타데이터 섹션 처리
                self._parse_metadata(f)

                # 데이터베이스 섹션 처리
                self._parse_databases(f)

                return self.data
        except FileNotFoundError:
            # 파일이 없으면 빈 데이터베이스로 처리
            return {}

    def _parse_metadata(self, f):
        # 메타데이터 파싱 로직
        while True:
            byte = f.read(1)
            if not byte or byte != b'\xFA':  # 0xFA는 메타데이터 시작 표시
                f.seek(-1, 1)  # 한 바이트 뒤로 이동
                break

            # 메타데이터 이름과 값 파싱
            name = self._parse_string(f)
            value = self._parse_string(f)
            # 메타데이터는 현재 사용하지 않으므로 무시

    def _parse_databases(self, f):
        while True:
            byte = f.read(1)
            if not byte:
                break

            if byte == b'\xFF':  # 파일 끝 표시
                # 체크섬은 무시
                break

            if byte == b'\xFE':  # 데이터베이스 시작 표시
                # 데이터베이스 인덱스 읽기
                db_index = self._parse_size(f)

                # Hash 테이블 크기 정보 읽기
                if f.read(1) == b'\xFB':
                    hash_table_size = self._parse_size(f)
                    expires_size = self._parse_size(f)

                # 키-값 쌍 파싱
                self._parse_key_values(f)

    def _parse_key_values(self, f):
        while True:
            byte = f.read(1)
            if not byte or byte in [b'\xFE', b'\xFF']:
                f.seek(-1, 1)  # 한 바이트 뒤로 이동
                break

            # 만료 시간 처리
            expiry = None
            if byte == b'\xFD':  # 초 단위 만료 시간
                expiry_bytes = f.read(4)
                expiry = int.from_bytes(expiry_bytes, byteorder='little')
                byte = f.read(1)  # 다음 바이트(값 타입) 읽기
            elif byte == b'\xFC':  # 밀리초 단위 만료 시간
                expiry_bytes = f.read(8)
                expiry = int.from_bytes(expiry_bytes, byteorder='little') // 1000  # 초 단위로 변환
                byte = f.read(1)  # 다음 바이트(값 타입) 읽기

            # 값 타입 처리 (현재는 문자열(0x00)만 처리)
            value_type = byte[0]
            if value_type != 0:
                raise ValueError(f"Unsupported value type: {value_type}")

            # 키와 값 파싱
            key = self._parse_string(f)
            value = self._parse_string(f)

            # 데이터 저장 (만료 시간 포함)
            if expiry:
                self.data[key] = (value, datetime.datetime.fromtimestamp(expiry))
            else:
                self.data[key] = (value, -1)

    def _parse_size(self, f):
        """크기 인코딩 파싱"""
        byte = f.read(1)[0]

        # 첫 2비트에 따라 처리
        first_two_bits = byte >> 6
        if first_two_bits == 0:  # 0b00
            return byte & 0x3F  # 하위 6비트
        elif first_two_bits == 1:  # 0b01
            next_byte = f.read(1)[0]
            return ((byte & 0x3F) << 8) | next_byte
        elif first_two_bits == 2:  # 0b10
            # 4바이트 빅엔디안 값
            return int.from_bytes(f.read(4), byteorder='big')
        else:  # 0b11 - 특수 문자열 인코딩
            return None  # 문자열 처리에서 별도로 처리

    def _parse_string(self, f):
        """문자열 인코딩 파싱"""
        byte = f.read(1)[0]

        # 첫 2비트에 따라 처리
        first_two_bits = byte >> 6
        if first_two_bits != 3:  # 0b11이 아닌 경우
            # 일반 문자열 - 길이를 먼저 구함
            if first_two_bits == 0:  # 0b00
                length = byte & 0x3F
            elif first_two_bits == 1:  # 0b01
                next_byte = f.read(1)[0]
                length = ((byte & 0x3F) << 8) | next_byte
            elif first_two_bits == 2:  # 0b10
                length = int.from_bytes(f.read(4), byteorder='big')

            # 문자열 읽기
            return f.read(length).decode('utf-8')
        else:  # 0b11 - 특수 인코딩
            encoding_type = byte & 0x3F
            if encoding_type == 0:  # 8비트 정수
                return str(f.read(1)[0])
            elif encoding_type == 1:  # 16비트 정수
                return str(int.from_bytes(f.read(2), byteorder='little'))
            elif encoding_type == 2:  # 32비트 정수
                return str(int.from_bytes(f.read(4), byteorder='little'))
            else:
                raise ValueError(f"Unsupported string encoding: {encoding_type}")