"""数据卷文件操作：内容寻址、原子落盘、安全读取与删除。"""
from __future__ import annotations

import hashlib
import os
import tempfile
import uuid
from pathlib import Path
from typing import BinaryIO, Tuple

from .config import Settings, settings as default_settings


class UploadTooLarge(Exception):
    """上传字节数超过配置上限。"""


def blob_path(sha256: str, cfg: Settings = default_settings) -> Path:
    # 两级目录分片，避免单目录文件过多
    return cfg.blobs_dir / sha256[:2] / sha256[2:4] / sha256


def stream_to_tmp(
    source: BinaryIO, cfg: Settings = default_settings, chunk_size: int = 1024 * 1024
) -> Tuple[str, int, Path]:
    """流式读取上传内容到临时文件，同时计算 SHA-256。

    成功返回 (sha256, size, tmp_path)；超限抛出 :class:`UploadTooLarge`，
    临时文件由调用方在失败时清理（这里也会尽力清理）。
    """
    digest = hashlib.sha256()
    total = 0
    fd, tmp_name = tempfile.mkstemp(prefix="upload-", dir=cfg.tmp_dir)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as out:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                total += len(chunk)
                if total > cfg.max_upload_bytes:
                    raise UploadTooLarge(f"upload exceeds {cfg.max_upload_bytes} bytes")
                digest.update(chunk)
                out.write(chunk)
            out.flush()
            os.fsync(out.fileno())
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return digest.hexdigest(), total, tmp_path


def publish_blob(tmp_path: Path, sha256: str, cfg: Settings = default_settings) -> Path:
    """把临时文件原子地发布为数据卷文件。

    同内容（同 SHA-256）共用同一文件：目标已存在时直接丢弃临时文件。
    os.replace 是同文件系统内的原子操作，任何时刻读者要么看不到该文件，
    要么看到完整文件。
    """
    target = blob_path(sha256, cfg)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        tmp_path.unlink(missing_ok=True)
        return target
    # 先在同目录放一个唯一名，再 replace，避免并发发布时互相干扰
    staged = target.parent / f".staging-{uuid.uuid4().hex}"
    os.replace(tmp_path, staged)
    try:
        os.replace(staged, target)
    except FileExistsError:
        staged.unlink(missing_ok=True)
    return target


def discard_tmp(tmp_path: Path) -> None:
    tmp_path.unlink(missing_ok=True)


def open_blob(sha256: str, cfg: Settings = default_settings) -> BinaryIO:
    """以只读方式打开数据卷文件（调用方负责关闭）。"""
    return open(blob_path(sha256, cfg), "rb")


def remove_blob_file(sha256: str, cfg: Settings = default_settings) -> bool:
    """删除数据卷文件，返回是否真的执行了删除。

    仅在 gc 临界区（引用 + 租约重新校验通过后）调用。文件不存在视为已删除，
    保证“删除失败/中断后重试”可以幂等推进。
    """
    path = blob_path(sha256, cfg)
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    # 清理可能留下的空分片目录（非空时静默跳过）
    for parent in (path.parent, path.parent.parent):
        try:
            parent.rmdir()
        except OSError:
            pass
    return True


def list_blob_files(cfg: Settings = default_settings):
    """枚举数据卷上现存的 blob 文件（用于重启后的对账）。"""
    if not cfg.blobs_dir.exists():
        return []
    return [p.name for p in cfg.blobs_dir.glob("*/*/*") if p.is_file() and not p.name.startswith(".")]
