"""运行期配置：全部可通过环境变量覆盖，便于容器与测试使用。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    # 数据根目录：SQLite 数据库与 blobs 数据卷都在其下
    data_dir: Path = field(default_factory=lambda: Path(os.environ.get("DATA_DIR", "/data")))
    db_name: str = field(default_factory=lambda: os.environ.get("DB_NAME", "moisture.db"))
    blobs_dirname: str = "blobs"
    tmp_dirname: str = "tmp"
    # 下载租约的生命周期保护在进程内完成；此参数仅用于防御性清理
    lease_ttl_seconds: int = field(default_factory=lambda: _env_int("LEASE_TTL_SECONDS", 3600))
    # 触发回收后，遇到正在下载的文件时的重试次数（每次重新校验引用）
    gc_max_passes: int = field(default_factory=lambda: _env_int("GC_MAX_PASSES", 3))
    # 单次请求允许的最大 CSV 字节数（默认 64 MiB）
    max_upload_bytes: int = field(default_factory=lambda: _env_int("MAX_UPLOAD_BYTES", 64 * 1024 * 1024))

    @property
    def db_path(self) -> Path:
        return self.data_dir / self.db_name

    @property
    def blobs_dir(self) -> Path:
        return self.data_dir / self.blobs_dirname

    @property
    def tmp_dir(self) -> Path:
        return self.data_dir / self.tmp_dirname


settings = Settings()
