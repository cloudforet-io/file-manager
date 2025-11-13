"""
Improved Streaming File Connector Manager
동기 방식으로 안정적인 스트리밍 파일 처리 (메모리/타임아웃 최적화)
각 connector의 stream_upload_file 메서드를 활용한 구현
"""
import logging
from typing import Generator, BinaryIO, Optional
from io import BytesIO
import time

from spaceone.core import config
from spaceone.core.manager import BaseManager
from spaceone.file_manager.error import *

_LOGGER = logging.getLogger(__name__)

# 설정 상수
DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
PROGRESS_LOG_INTERVAL = 10 * 1024 * 1024  # 10MB마다 로깅


class StreamingFileConnectorManager(BaseManager):
    """
    동기 방식 스트리밍 파일 커넥터 매니저
    메모리 효율적인 청크 단위 처리 + 각 connector의 네이티브 스트리밍 활용
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        backend = config.get_global("BACKEND", "FileConnectorManager")
        connector_config = config.get_global("CONNECTORS", {}).get(backend, {})
        self.backend_type = connector_config.get("backend", backend).lower()

        # 동기 커넥터 초기화
        try:
            from spaceone.file_manager.connector.file_base_connector import FileBaseConnector
            self.file_conn: FileBaseConnector = self.locator.get_connector(backend)
        except Exception as e:
            _LOGGER.error(f"[StreamingFileConnectorManager] Failed to initialize connector {backend}: {e}")
            raise ERROR_NOT_DEFINED_FILE_BACKEND(backend=backend)

        _LOGGER.info(f"[StreamingFileConnectorManager] Initialized with backend: {self.backend_type}")

    def check_file(self, resource_group: str, file_id: str) -> bool:
        """파일 존재 여부 확인"""
        try:
            return self.file_conn.check_file(resource_group, file_id)
        except Exception as e:
            _LOGGER.error(f"[check_file] Error checking file {file_id}: {e}")
            return False

    def delete_file(self, resource_group: str, file_id: str) -> None:
        """파일 삭제"""
        try:
            self.file_conn.delete_file(resource_group, file_id)
        except Exception as e:
            _LOGGER.error(f"[delete_file] Error deleting file {file_id}: {e}")
            raise

    def stream_upload_file(self, resource_group: str, file_id: str, file_obj) -> None:
        """
        스트리밍 업로드 (메모리 효율적, 타임아웃 안전)
        각 connector의 stream_upload_file 메서드를 직접 활용
        """
        _LOGGER.info(f"[stream_upload_file] Starting streaming upload for {file_id} (backend: {self.backend_type})")

        try:
            # connector가 stream_upload_file을 지원하면 직접 사용
            if hasattr(self.file_conn, 'stream_upload_file'):
                self.file_conn.stream_upload_file(resource_group, file_id, file_obj)
            else:
                # stream_upload_file을 지원하지 않으면 일반 upload_file 사용
                _LOGGER.warning(f"[stream_upload_file] Connector {type(self.file_conn).__name__} does not support stream_upload_file, falling back to upload_file")
                self._fallback_upload(resource_group, file_id, file_obj)

        except Exception as e:
            _LOGGER.error(f"[stream_upload_file] Upload failed for {file_id}: {e}")
            raise

    def _fallback_upload(self, resource_group: str, file_id: str, file_obj) -> None:
        """
        폴백: 청크 단위로 읽어서 upload_file 사용
        메모리 효율적 처리
        """
        _LOGGER.info(f"[_fallback_upload] Using chunked fallback upload for {file_id}")

        try:
            chunk_size = DEFAULT_CHUNK_SIZE
            total_size = 0
            last_log_size = 0

            # 파일 데이터를 청크 단위로 읽기
            while True:
                chunk = self._read_chunk(file_obj, chunk_size)
                if not chunk:
                    break

                # 직접 업로드
                self.file_conn.upload_file(resource_group, file_id, chunk)
                total_size += len(chunk)

                if total_size - last_log_size >= PROGRESS_LOG_INTERVAL:
                    _LOGGER.info(f"[_fallback_upload] Uploaded {total_size // (1024*1024)}MB")
                    last_log_size = total_size

            _LOGGER.info(f"[_fallback_upload] Upload completed. Total: {total_size // (1024*1024)}MB")

        except Exception as e:
            _LOGGER.error(f"[_fallback_upload] Error: {e}")
            raise

    def download_file_stream(self, resource_group: str, file_id: str) -> Generator[bytes, None, None]:
        """
        스트리밍 다운로드 (제너레이터로 청크 반환)
        각 connector의 download_file 메서드 활용
        """
        _LOGGER.info(f"[download_file_stream] Starting streaming download for {file_id}")

        try:
            # connector의 download_file 메서드 호출
            result = self.file_conn.download_file(resource_group, file_id)

            # 결과 타입에 따라 청크 단위로 yield
            chunk_size = 1024 * 1024  # 1MB

            if isinstance(result, dict) and 'Body' in result:
                # S3/MinIO 스타일 응답: {'Body': stream, 'ContentLength': size}
                body = result['Body']
                _LOGGER.info(f"[download_file_stream] S3-style response detected")

                if isinstance(body, BytesIO):
                    body.seek(0)  # 처음부터 읽기
                    while True:
                        chunk = body.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk
                    body.close()

                elif hasattr(body, 'read'):
                    # 스트림 객체
                    try:
                        while True:
                            chunk = body.read(chunk_size)
                            if not chunk:
                                break
                            yield chunk
                    finally:
                        if hasattr(body, 'close'):
                            body.close()
                        if hasattr(body, 'release_conn'):
                            body.release_conn()
                else:
                    # 기타: bytes 변환
                    data = bytes(body)
                    for i in range(0, len(data), chunk_size):
                        yield data[i:i+chunk_size]

            elif isinstance(result, bytes):
                # bytes 직접 반환
                _LOGGER.info(f"[download_file_stream] bytes response detected")
                for i in range(0, len(result), chunk_size):
                    yield result[i:i+chunk_size]

            elif hasattr(result, 'read'):
                # 스트림 객체
                _LOGGER.info(f"[download_file_stream] Stream object detected")
                try:
                    while True:
                        chunk = result.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk
                finally:
                    if hasattr(result, 'close'):
                        result.close()
                    if hasattr(result, 'release_conn'):
                        result.release_conn()
            else:
                # 기타
                _LOGGER.warning(f"[download_file_stream] Unknown result type: {type(result)}")
                yield bytes(result)

            _LOGGER.info(f"[download_file_stream] Download completed")

        except Exception as e:
            _LOGGER.error(f"[download_file_stream] Download failed: {e}")
            raise

    # ===== 유틸리티 함수 =====

    def _get_file_stream(self, file_obj) -> BinaryIO:
        """
        다양한 파일 객체 타입을 표준 스트림으로 변환
        FastAPI UploadFile, 파일 객체, BytesIO 등 모두 지원
        """
        if hasattr(file_obj, 'file'):
            # FastAPI UploadFile
            return file_obj.file
        elif isinstance(file_obj, BytesIO):
            # 이미 BytesIO
            return file_obj
        elif hasattr(file_obj, 'read') and hasattr(file_obj, 'seek'):
            # 파일 객체
            return file_obj
        else:
            # 기타: bytes를 BytesIO로 변환
            return BytesIO(file_obj)

    def _read_chunk(self, file_obj, chunk_size: int) -> bytes:
        """
        파일 객체에서 안전하게 청크 읽기
        FastAPI UploadFile, 파일 객체 모두 지원
        """
        try:
            if hasattr(file_obj, 'file'):
                # FastAPI UploadFile
                chunk = file_obj.file.read(chunk_size)
                return chunk if chunk else b''
            elif hasattr(file_obj, 'read'):
                # 파일 객체
                chunk = file_obj.read(chunk_size)
                return chunk if chunk else b''
            else:
                # 기타
                return b''
        except Exception as e:
            _LOGGER.error(f"[_read_chunk] Error reading chunk: {e}")
            raise
