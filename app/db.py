"""SQLite 连接、模式初始化与并发原语。

设计要点：
- WAL + busy_timeout，读写并发；
- 外键约束开启，引用完整性由事务保证；
- 进程内一把可重入锁 ``metadata_lock`` 只保护“短临界区”（登记/引用判定/
  unlink 前最后校验），不跨网络流式传输持有，慢下载因此不会阻塞上传、释放
  或其他文件的回收。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import Settings, settings as default_settings

# 所有变更操作在此锁内完成 SQLite 事务（临界区很短，不包含文件流式 I/O）
metadata_lock = threading.RLock()


SCHEMA = """
CREATE TABLE IF NOT EXISTS blobs (
    sha256     TEXT PRIMARY KEY,
    size      INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS partitions (
    partition_id TEXT PRIMARY KEY,
    current_blob TEXT,
    row_count    INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    FOREIGN KEY (current_blob) REFERENCES blobs(sha256) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS partition_entries (
    partition_id TEXT NOT NULL,
    row_index    INTEGER NOT NULL,
    station      TEXT NOT NULL,
    moisture     REAL NOT NULL,
    blob_sha     TEXT NOT NULL,
    PRIMARY KEY (partition_id, row_index),
    FOREIGN KEY (partition_id) REFERENCES partitions(partition_id),
    FOREIGN KEY (blob_sha) REFERENCES blobs(sha256)
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at  TEXT NOT NULL,
    released_at TEXT
);

CREATE TABLE IF NOT EXISTS snapshot_entries (
    snapshot_id  INTEGER NOT NULL,
    partition_id TEXT NOT NULL,
    blob_sha     TEXT NOT NULL,
    row_count    INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, partition_id),
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id),
    FOREIGN KEY (blob_sha) REFERENCES blobs(sha256)
);

CREATE TABLE IF NOT EXISTS download_leases (
    lease_id   TEXT PRIMARY KEY,
    blob_sha   TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    FOREIGN KEY (blob_sha) REFERENCES blobs(sha256)
);

CREATE TABLE IF NOT EXISTS gc_queue (
    sha256      TEXT PRIMARY KEY,
    enqueued_at TEXT NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT
);

-- 快照用量账本：按（快照, 分区）长期保留，独立于 snapshot_entries。
-- 快照释放或 blob 原件被回收时，snapshot_entries 可能被删除，但这里的历史
-- 统计必须仍可查询；因此只对 snapshots 建外键，不对 blobs/snapshot_entries
-- 建外键。每次“完整发送”只用一个原子 UPDATE 记账，失败整体回滚。
CREATE TABLE IF NOT EXISTS snapshot_usage (
    snapshot_id         INTEGER NOT NULL,
    partition_id        TEXT NOT NULL,
    blob_sha            TEXT NOT NULL,
    size_bytes          INTEGER NOT NULL DEFAULT 0,
    completed_downloads INTEGER NOT NULL DEFAULT 0,
    total_bytes         INTEGER NOT NULL DEFAULT 0,
    last_completed_at   TEXT,
    PRIMARY KEY (snapshot_id, partition_id),
    FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_partition_entries_blob
    ON partition_entries(blob_sha);
CREATE INDEX IF NOT EXISTS idx_snapshot_entries_blob
    ON snapshot_entries(blob_sha);
CREATE INDEX IF NOT EXISTS idx_leases_blob
    ON download_leases(blob_sha);
CREATE INDEX IF NOT EXISTS idx_snapshot_usage_snapshot
    ON snapshot_usage(snapshot_id);
"""


def connect(cfg: Settings = default_settings) -> sqlite3.Connection:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        cfg.db_path,
        timeout=30.0,
        isolation_level=None,  # 显式 BEGIN/COMMIT
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=FULL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def init_db(cfg: Settings = default_settings) -> None:
    """建表并清理上次进程遗留的下载租约。

    租约只在本进程的流式响应期间有效；进程重启意味着没有任何下载还活着，
    旧租约一律作废，恢复后相关文件即可被正常回收。

    同时为历史快照补建用量账本行（只补零值，不猜测未完成下载）：
    账本是新增能力，升级前已创建的快照没有账本行，这里从仍存在的
    ``snapshot_entries`` 补出分区清单；补行幂等且不会改动已提交计数。
    """
    Path(cfg.data_dir).mkdir(parents=True, exist_ok=True)
    cfg.blobs_dir.mkdir(parents=True, exist_ok=True)
    cfg.tmp_dir.mkdir(parents=True, exist_ok=True)
    conn = connect(cfg)
    try:
        # executescript 会先隐式 COMMIT，因此 DDL 之后再开真正的写事务
        conn.executescript(SCHEMA)
        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO snapshot_usage(snapshot_id, partition_id, blob_sha, size_bytes)
                SELECT se.snapshot_id, se.partition_id, se.blob_sha,
                       COALESCE((SELECT b.size FROM blobs b WHERE b.sha256 = se.blob_sha), 0)
                  FROM snapshot_entries se
                """
            )
            conn.execute("DELETE FROM download_leases;")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """显式事务上下文（自动提交模式下手动管理）。"""
    conn.execute("BEGIN IMMEDIATE;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
