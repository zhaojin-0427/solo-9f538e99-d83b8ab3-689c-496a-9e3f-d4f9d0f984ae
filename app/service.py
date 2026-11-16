"""业务服务层：分区上传替换、快照固定、下载租约与安全回收。

引用模型
--------
一个 blob（SHA-256 标识的不可变文件）在以下任一位置被引用即不可回收：
  1. 某分区的当前版本（partitions.current_blob / partition_entries）；
  2. 任一未释放快照的条目（snapshot_entries，所属 snapshots.released_at IS NULL）。
回收前会在同一把 ``metadata_lock`` 临界区内重新校验引用与下载租约，
因此与上传、释放、下载、回收交错时仍有引用的文件绝不会被删除。
"""
from __future__ import annotations

import csv
import io
import logging
import math
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Sequence

import anyio

from . import storage
from .config import Settings, settings as default_settings
from .db import connect, metadata_lock, transaction

logger = logging.getLogger(__name__)


class ServiceError(Exception):
    """业务错误基类。"""


class CsvValidationError(ServiceError):
    """CSV 内容不符合要求。"""


class NotFound(ServiceError):
    """分区或快照不存在。"""


class SnapshotReleased(ServiceError):
    """快照已释放，拒绝新的读取。"""


class MissingBlobFile(ServiceError):
    """登记在册的 blob 在数据卷上缺失（数据卷损坏）。"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="seconds")


def _iso_us(ts: datetime) -> str:
    """微秒级时间戳，用于用量账本的“最近完成时间”。"""
    return ts.isoformat(timespec="microseconds")


@dataclass(frozen=True)
class DownloadTicket:
    """一次已受理下载的记账票据。

    ``sha``/``size`` 在受理时从数据库与数据卷读到并冻结，之后的分区替换或
    文件回收都不影响本次下载的计量：即使快照已释放、原件已被回收，只要这次
    受理的下载最终读完，仍按受理时固定的字节数入账一次。
    ``snapshot_id`` 为 ``None`` 表示普通（当前分区）下载，不记快照用量。
    ``path`` 同样是受理时解析出的数据卷路径，流式读取不再查库。
    """

    snapshot_id: Optional[int]
    partition_id: str
    sha: str
    size: int
    path: Path
    lease_id: str


# --------------------------------------------------------------------------- #
# CSV 解析
# --------------------------------------------------------------------------- #
def parse_csv_bytes(data: bytes) -> List[Dict[str, object]]:
    """解析上传的 CSV。

    要求：UTF-8（允许 BOM）；首行表头必须包含“站点”和“湿度”两列
    （大小写/前后空格不敏感，兼容 station / moisture 英文名）；
    湿度必须是有限数值；站点不能为空。返回 [{"station", "moisture"}, ...]。
    """
    if not data:
        raise CsvValidationError("CSV 内容为空")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError(f"CSV 必须是 UTF-8 编码: {exc}") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise CsvValidationError("CSV 缺少表头") from exc

    normalized = [h.strip().lower() for h in header]
    station_aliases = {"站点", "station", "site", "station_id", "site_id"}
    moisture_aliases = {"湿度", "moisture", "soil_moisture", "sm"}
    try:
        station_idx = next(i for i, h in enumerate(normalized) if h in station_aliases)
        moisture_idx = next(i for i, h in enumerate(normalized) if h in moisture_aliases)
    except StopIteration as exc:
        raise CsvValidationError(
            f"表头必须包含站点列和湿度列，实际表头: {header}"
        ) from exc

    rows: List[Dict[str, object]] = []
    for line_no, raw in enumerate(reader, start=2):
        if not raw or all(not c.strip() for c in raw):
            continue  # 容忍空行
        if max(station_idx, moisture_idx) >= len(raw):
            raise CsvValidationError(f"第 {line_no} 行列数不足: {raw}")
        station = raw[station_idx].strip()
        if not station:
            raise CsvValidationError(f"第 {line_no} 行站点为空")
        moisture_raw = raw[moisture_idx].strip()
        try:
            moisture = float(moisture_raw)
        except ValueError as exc:
            raise CsvValidationError(
                f"第 {line_no} 行湿度不是数值: {moisture_raw!r}"
            ) from exc
        if not math.isfinite(moisture):
            raise CsvValidationError(f"第 {line_no} 行湿度不是有限数值: {moisture_raw!r}")
        rows.append({"station": station, "moisture": moisture})

    if not rows:
        raise CsvValidationError("CSV 没有任何数据行")
    return rows


# --------------------------------------------------------------------------- #
# 服务
# --------------------------------------------------------------------------- #
class MoistureService:
    def __init__(self, cfg: Settings = default_settings):
        self.cfg = cfg
        # 进程内活动下载租约计数：lease_id -> blob_sha
        self._active_readers: Dict[str, str] = {}
        self._readers_lock = threading.Lock()
        # 活动下载流的强引用集合，防止异步生成器在响应结束前被 GC
        # （否则其 finally 会提前释放租约）
        self._active_streams: set = set()

    # -- 基础工具 ---------------------------------------------------------- #
    def _conn(self) -> sqlite3.Connection:
        return connect(self.cfg)

    def startup(self) -> None:
        """建表、清理过期临时文件、恢复未完成的回收。"""
        from .db import init_db

        init_db(self.cfg)
        self._cleanup_tmp()
        self.reconcile_disk()
        self.run_gc()

    def _cleanup_tmp(self) -> None:
        tmp = self.cfg.tmp_dir
        if not tmp.exists():
            return
        for p in tmp.iterdir():
            if p.is_file():
                p.unlink(missing_ok=True)

    # -- 引用判定（必须在 metadata_lock 下调用） --------------------------- #
    def _is_referenced(self, conn: sqlite3.Connection, sha: str) -> bool:
        row = conn.execute(
            """
            SELECT
                EXISTS(SELECT 1 FROM partitions WHERE current_blob = ?)
              OR EXISTS(SELECT 1 FROM partition_entries WHERE blob_sha = ?)
              OR EXISTS(
                    SELECT 1 FROM snapshot_entries se
                    JOIN snapshots s ON s.snapshot_id = se.snapshot_id
                    WHERE se.blob_sha = ? AND s.released_at IS NULL)
            AS referenced
            """,
            (sha, sha, sha),
        ).fetchone()
        return bool(row["referenced"])

    def _has_lease(self, conn: sqlite3.Connection, sha: str) -> bool:
        # 仅统计未过期租约；过期租约由 sweep_expired_leases 定期清理
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM download_leases"
            " WHERE blob_sha = ? AND expires_at > ?",
            (sha, _iso(_utcnow())),
        ).fetchone()
        return row["n"] > 0

    def sweep_expired_leases(self) -> int:
        """删除超过 TTL 的租约（防御进程崩溃遗留的死租约）。"""
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    cur = conn.execute(
                        "DELETE FROM download_leases WHERE expires_at <= ?",
                        (_iso(_utcnow()),),
                    )
                    return cur.rowcount
            finally:
                conn.close()

    def _enqueue_gc(self, conn: sqlite3.Connection, shas: Sequence[str]) -> None:
        now = _iso(_utcnow())
        for sha in shas:
            if not sha:
                continue
            # 重新引用（同内容再次上传）时旧队列项作废
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha,))
            conn.execute(
                "INSERT INTO gc_queue(sha256, enqueued_at, attempts) VALUES (?, ?, 0)",
                (sha, now),
            )

    # -- 上传：整份替换当前分区 ------------------------------------------- #
    def upload_partition(
        self, partition_id: str, stream: BinaryIO
    ) -> Dict[str, object]:
        """按日期分区上传 CSV，整份替换当前分区。

        流程：先把字节完整落到临时文件并算出 SHA-256（此阶段不触碰任何
        现有登记），然后在一个事务里登记 blob + 替换分区；任何一步失败
        原分区保持不变。旧版本 blob 进入回收队列，其文件在重新校验引用
        （含未释放快照）后才会被删除。
        """
        sha, size, tmp_path = storage.stream_to_tmp(stream, self.cfg)
        replaced = False
        published = False
        try:
            # 解析在事务外完成：校验失败时绝不动现有分区
            with open(tmp_path, "rb") as fh:
                rows = parse_csv_bytes(fh.read())

            now = _iso(_utcnow())
            with metadata_lock:
                conn = self._conn()
                try:
                    with transaction(conn):
                        # 登记 blob（若同 SHA 已存在则复用，实现数据卷共用）
                        existing = conn.execute(
                            "SELECT 1 FROM blobs WHERE sha256 = ?", (sha,)
                        ).fetchone()
                        if existing:
                            # 同内容重新上传/仍被引用：取消任何待回收记录
                            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha,))
                        else:
                            conn.execute(
                                "INSERT INTO blobs(sha256, size, created_at) VALUES (?, ?, ?)",
                                (sha, size, now),
                            )
                        storage.publish_blob(tmp_path, sha, self.cfg)
                        published = True

                        old = conn.execute(
                            "SELECT current_blob FROM partitions WHERE partition_id = ?",
                            (partition_id,),
                        ).fetchone()
                        replaced = old is not None
                        if old is None:
                            conn.execute(
                                "INSERT INTO partitions(partition_id, current_blob,"
                                " row_count, created_at, updated_at)"
                                " VALUES (?, ?, ?, ?, ?)",
                                (partition_id, sha, len(rows), now, now),
                            )
                        else:
                            conn.execute(
                                "UPDATE partitions SET current_blob = ?, row_count = ?,"
                                " updated_at = ? WHERE partition_id = ?",
                                (sha, len(rows), now, partition_id),
                            )
                            conn.execute(
                                "DELETE FROM partition_entries WHERE partition_id = ?",
                                (partition_id,),
                            )
                        conn.executemany(
                            "INSERT INTO partition_entries(partition_id, row_index,"
                            " station, moisture, blob_sha) VALUES (?, ?, ?, ?, ?)",
                            [
                                (partition_id, i, r["station"], r["moisture"], sha)
                                for i, r in enumerate(rows)
                            ],
                        )
                        # 注：快照用量账本按（快照, 分区）冻结 SHA-256，不随
                        # 分区后续替换而改写，因此这里无需触碰 snapshot_usage。
                        orphaned: List[str] = []
                        if old is not None and old["current_blob"]:
                            old_sha = old["current_blob"]
                            if old_sha != sha and not self._is_referenced(conn, old_sha):
                                orphaned.append(old_sha)
                        self._enqueue_gc(conn, orphaned)
                finally:
                    conn.close()
        finally:
            if not published:
                storage.discard_tmp(tmp_path)

        return {
            "partition_id": partition_id,
            "sha256": sha,
            "size": size,
            "row_count": len(rows),
            "replaced": replaced,
        }

    # -- 快照：固定全部当前分区 ------------------------------------------- #
    def create_snapshot(self) -> Dict[str, object]:
        now = _iso(_utcnow())
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    cur = conn.execute(
                        "INSERT INTO snapshots(created_at) VALUES (?)", (now,)
                    )
                    snapshot_id = cur.lastrowid
                    conn.execute(
                        """
                        INSERT INTO snapshot_entries(snapshot_id, partition_id, blob_sha, row_count)
                        SELECT ?, partition_id, current_blob, row_count
                          FROM partitions
                         WHERE current_blob IS NOT NULL
                        """,
                        (snapshot_id,),
                    )
                    # 为本次固定的每个分区建立用量账本行（含从未下载者也列出）
                    conn.execute(
                        """
                        INSERT INTO snapshot_usage(snapshot_id, partition_id, blob_sha,
                                                   size_bytes, completed_downloads, total_bytes)
                        SELECT ?, p.partition_id, p.current_blob, COALESCE(b.size, 0), 0, 0
                          FROM partitions p
                          LEFT JOIN blobs b ON b.sha256 = p.current_blob
                         WHERE p.current_blob IS NOT NULL
                        """,
                        (snapshot_id,),
                    )
                    n = conn.execute(
                        "SELECT COUNT(*) AS n FROM snapshot_entries WHERE snapshot_id = ?",
                        (snapshot_id,),
                    ).fetchone()["n"]
                return {"snapshot_id": snapshot_id, "created_at": now, "partition_count": n}
            finally:
                conn.close()

    def _get_live_snapshot(self, conn: sqlite3.Connection, snapshot_id: int) -> sqlite3.Row:
        snap = conn.execute(
            "SELECT * FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()
        if snap is None:
            raise NotFound(f"快照 {snapshot_id} 不存在")
        if snap["released_at"] is not None:
            raise SnapshotReleased(f"快照 {snapshot_id} 已释放，拒绝新的读取")
        return snap

    def release_snapshot(self, snapshot_id: int) -> Dict[str, object]:
        now = _iso(_utcnow())
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    self._get_live_snapshot(conn, snapshot_id)
                    conn.execute(
                        "UPDATE snapshots SET released_at = ? WHERE snapshot_id = ?",
                        (now, snapshot_id),
                    )
                    # 该快照独占的 blob 立刻进入回收队列（仍有租约者稍后重试）
                    candidates = conn.execute(
                        "SELECT DISTINCT blob_sha FROM snapshot_entries WHERE snapshot_id = ?",
                        (snapshot_id,),
                    ).fetchall()
                    orphaned = [
                        r["blob_sha"]
                        for r in candidates
                        if not self._is_referenced(conn, r["blob_sha"])
                    ]
                    self._enqueue_gc(conn, orphaned)
                return {"snapshot_id": snapshot_id, "released_at": now}
            finally:
                conn.close()

    def list_snapshots(self) -> List[Dict[str, object]]:
        conn = self._conn()
        try:
            rows = conn.execute(
                """
                SELECT s.snapshot_id, s.created_at, s.released_at,
                       (SELECT COUNT(*) FROM snapshot_entries se
                         WHERE se.snapshot_id = s.snapshot_id) AS partition_count
                  FROM snapshots s ORDER BY s.snapshot_id
                """
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # -- 下载 -------------------------------------------------------------- #
    def _acquire_lease(self, sha: str) -> str:
        lease_id = uuid.uuid4().hex
        now = _utcnow()
        expires = now + timedelta(seconds=self.cfg.lease_ttl_seconds)
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    conn.execute(
                        "INSERT INTO download_leases(lease_id, blob_sha, created_at, expires_at)"
                        " VALUES (?, ?, ?, ?)",
                        (lease_id, sha, _iso(now), _iso(expires)),
                    )
            finally:
                conn.close()
        with self._readers_lock:
            self._active_readers[lease_id] = sha
        return lease_id

    def _release_lease(self, lease_id: str) -> None:
        with metadata_lock:
            conn = self._conn()
            try:
                conn.execute("DELETE FROM download_leases WHERE lease_id = ?", (lease_id,))
            finally:
                conn.close()
        with self._readers_lock:
            self._active_readers.pop(lease_id, None)

    def _resolve_partition_blob(
        self, conn: sqlite3.Connection, partition_id: str, snapshot_id: Optional[int]
    ) -> str:
        if snapshot_id is not None:
            self._get_live_snapshot(conn, snapshot_id)
            row = conn.execute(
                "SELECT blob_sha FROM snapshot_entries"
                " WHERE snapshot_id = ? AND partition_id = ?",
                (snapshot_id, partition_id),
            ).fetchone()
            if row is None:
                raise NotFound(
                    f"快照 {snapshot_id} 中没有分区 {partition_id}"
                )
            return row["blob_sha"]
        row = conn.execute(
            "SELECT current_blob FROM partitions WHERE partition_id = ?",
            (partition_id,),
        ).fetchone()
        if row is None or row["current_blob"] is None:
            raise NotFound(f"分区 {partition_id} 不存在")
        return row["current_blob"]

    def open_partition_download(
        self, partition_id: str, snapshot_id: Optional[int] = None
    ) -> DownloadTicket:
        """受理一次下载：解析固定版本、取得租约并冻结计量票据。

        租约在打开文件前取得：回收临界区会跳过带租约的 blob，因此已开始
        的下载一定能完整结束。流式读取期间不持有 metadata_lock。
        票据里的 sha/字节数在受理时固定，调用方在流正常读完时把它交给
        :meth:`record_completed_download` 入账。
        """
        with metadata_lock:
            conn = self._conn()
            try:
                sha = self._resolve_partition_blob(conn, partition_id, snapshot_id)
                path = storage.blob_path(sha, self.cfg)
                try:
                    size = path.stat().st_size
                except FileNotFoundError:
                    raise MissingBlobFile(f"blob {sha} 的数据卷文件缺失") from None
                lease_id = self._acquire_lease(sha)
            finally:
                conn.close()

        return DownloadTicket(
            snapshot_id=snapshot_id,
            partition_id=partition_id,
            sha=sha,
            size=size,
            path=path,
            lease_id=lease_id,
        )

    def release_download(self, lease_id: str) -> None:
        """释放下载租约（响应结束、含客户端中途断开时调用）。"""
        self._release_lease(lease_id)

    def record_completed_download(self, ticket: DownloadTicket) -> bool:
        """一次快照下载“正常读到末尾”后入账（单事务、全有或全无）。

        - 仅快照下载入账；普通当前分区下载不记快照用量；
        - 只有客户端把响应完整读完（服务端把最后一字节交给 ASGI 服务器）
          才会调用到这里，首块前断开或中途断开都不入账；
        - 并发下载各自持有票据，以原子 ``UPDATE ... + 1`` 分别累加；
        - 账本行不依赖 blob 原件与快照条目：快照释放、原件回收后历史仍可查；
          释放前受理的下载即使随后读完，仍照常累加；
        - 账本行缺失（不应发生）时补建一行再累加，保证“不丢笔”；
        - 任何写账失败整体回滚，不留下半笔记录；错误由调用方记录日志。
        """
        if ticket.snapshot_id is None:
            return False
        now = _iso_us(_utcnow())
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    cur = conn.execute(
                        """
                        UPDATE snapshot_usage
                           SET completed_downloads = completed_downloads + 1,
                               total_bytes = total_bytes + ?,
                               last_completed_at = ?
                         WHERE snapshot_id = ? AND partition_id = ?
                        """,
                        (ticket.size, now, ticket.snapshot_id, ticket.partition_id),
                    )
                    if cur.rowcount == 0:
                        # 防御性补行：账本行应在建快照时写入
                        conn.execute(
                            """
                            INSERT INTO snapshot_usage(snapshot_id, partition_id, blob_sha,
                                                       size_bytes, completed_downloads,
                                                       total_bytes, last_completed_at)
                            VALUES (?, ?, ?, ?, 1, ?, ?)
                            """,
                            (
                                ticket.snapshot_id,
                                ticket.partition_id,
                                ticket.sha,
                                ticket.size,
                                ticket.size,
                                now,
                            ),
                        )
                return True
            finally:
                conn.close()

    def get_snapshot_usage(self, snapshot_id: int) -> Dict[str, object]:
        """逐分区列出快照固定时的 SHA-256、完整发送次数、累计字节与最近完成时间。

        包含从未下载的分区（计数 0、字节 0、时间为 null）。数据来自长期账本，
        快照释放及原件回收后仍可查询。
        """
        conn = self._conn()
        try:
            snap = conn.execute(
                "SELECT snapshot_id, created_at, released_at FROM snapshots"
                " WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
            if snap is None:
                raise NotFound(f"快照 {snapshot_id} 不存在")
            rows = conn.execute(
                """
                SELECT u.partition_id, u.blob_sha AS sha256,
                       u.size_bytes,
                       u.completed_downloads, u.total_bytes, u.last_completed_at,
                       EXISTS(SELECT 1 FROM blobs b WHERE b.sha256 = u.blob_sha)
                           AS blob_retained
                  FROM snapshot_usage u
                 WHERE u.snapshot_id = ?
                 ORDER BY u.partition_id
                """,
                (snapshot_id,),
            ).fetchall()
            partitions = [
                {
                    "partition_id": r["partition_id"],
                    "sha256": r["sha256"],
                    "size_bytes": r["size_bytes"],
                    "completed_downloads": r["completed_downloads"],
                    "total_bytes": r["total_bytes"],
                    "last_completed_at": r["last_completed_at"],
                    # 原件是否仍在数据卷登记中（回收后仍可查历史用量）
                    "blob_retained": bool(r["blob_retained"]),
                }
                for r in rows
            ]
            return {
                "snapshot_id": snap["snapshot_id"],
                "created_at": snap["created_at"],
                "released_at": snap["released_at"],
                "partition_count": len(partitions),
                "partitions": partitions,
            }
        finally:
            conn.close()

    # -- 当前分区元数据 ---------------------------------------------------- #
    def get_partition(self, partition_id: str) -> Dict[str, object]:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM partitions WHERE partition_id = ?", (partition_id,)
            ).fetchone()
            if row is None:
                raise NotFound(f"分区 {partition_id} 不存在")
            return dict(row)
        finally:
            conn.close()

    def list_partitions(self) -> List[Dict[str, object]]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT partition_id, current_blob, row_count, created_at, updated_at"
                " FROM partitions ORDER BY partition_id"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # -- 回收 -------------------------------------------------------------- #
    def reconcile_disk(self) -> Dict[str, int]:
        """重启/启动对账：把数据卷上无登记的 blob 纳入回收队列。

        登记存在但文件缺失的情况不做破坏性处理，只通过下载时报错暴露。
        """
        enqueued = 0
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    known = {r["sha256"] for r in conn.execute("SELECT sha256 FROM blobs")}
                    on_disk = set(storage.list_blob_files(self.cfg))
                    now = _iso(_utcnow())
                    for sha in on_disk - known:
                        conn.execute(
                            "INSERT OR IGNORE INTO gc_queue(sha256, enqueued_at, attempts)"
                            " VALUES (?, ?, 0)",
                            (sha, now),
                        )
                        enqueued += 1
            finally:
                conn.close()
        return {"orphan_files_enqueued": enqueued}

    def run_gc(self) -> Dict[str, object]:
        """处理回收队列，可安全反复调用、可在重启后继续。

        每个候选 blob 在真正 unlink 前于同一临界区内重新校验：
        仍被当前分区/未释放快照引用 -> 出队保留；
        仍有下载租约 -> 留在队列等下一轮（慢下载不阻塞其他文件回收）；
        否则删除文件并移除登记。任何单文件失败只记录错误，不影响其他文件。
        """
        deleted: List[str] = []
        kept_referenced: List[str] = []
        busy: List[str] = []
        failed: List[Dict[str, str]] = []

        # 先清扫崩溃遗留的过期死租约，避免它们永久阻塞回收
        self.sweep_expired_leases()

        # 多轮处理：让“租约刚结束”的文件在同一次调用内尽快回收；
        # 仍在下载的文件留在队列，且不阻塞其他文件的删除。
        for _ in range(max(1, self.cfg.gc_max_passes)):
            busy_this_pass: List[str] = []
            with metadata_lock:
                conn = self._conn()
                try:
                    queued = conn.execute(
                        "SELECT sha256 FROM gc_queue ORDER BY enqueued_at"
                    ).fetchall()
                    for item in queued:
                        sha = item["sha256"]
                        # 每文件一个独立短事务 + 短临界区
                        try:
                            with transaction(conn):
                                referenced = self._is_referenced(conn, sha)
                                leased = self._has_lease(conn, sha)
                                blob_exists = conn.execute(
                                    "SELECT 1 FROM blobs WHERE sha256 = ?", (sha,)
                                ).fetchone()
                                if referenced:
                                    conn.execute(
                                        "DELETE FROM gc_queue WHERE sha256 = ?", (sha,)
                                    )
                                    kept_referenced.append(sha)
                                    continue
                                if leased:
                                    conn.execute(
                                        "UPDATE gc_queue SET attempts = attempts + 1,"
                                        " last_error = 'active download lease'"
                                        " WHERE sha256 = ?",
                                        (sha,),
                                    )
                                    busy_this_pass.append(sha)
                                    continue
                                # 引用与租约都为空：在同一临界区内 unlink，
                                # 之后任何并发上传都不可能再让“旧文件”复活，
                                # 同内容重新上传会走全新的原子发布。
                                # 先清理已释放快照留下的条目（其快照已不可读，
                                # 这些行不再构成引用；不清理会触发外键约束）。
                                conn.execute(
                                    """
                                    DELETE FROM snapshot_entries
                                     WHERE blob_sha = ?
                                       AND snapshot_id IN (
                                           SELECT snapshot_id FROM snapshots
                                            WHERE released_at IS NOT NULL)
                                    """,
                                    (sha,),
                                )
                                storage.remove_blob_file(sha, self.cfg)
                                if blob_exists:
                                    conn.execute(
                                        "DELETE FROM blobs WHERE sha256 = ?", (sha,)
                                    )
                                conn.execute(
                                    "DELETE FROM gc_queue WHERE sha256 = ?", (sha,)
                                )
                                deleted.append(sha)
                        except Exception as exc:
                            # 上面的事务已回滚；在新事务里记录错误，留待下次重试
                            try:
                                with transaction(conn):
                                    conn.execute(
                                        "UPDATE gc_queue SET attempts = attempts + 1,"
                                        " last_error = ? WHERE sha256 = ?",
                                        (repr(exc), sha),
                                    )
                            except Exception:
                                pass
                            failed.append({"sha256": sha, "error": repr(exc)})
                finally:
                    conn.close()
            busy.extend(busy_this_pass)
            if not busy_this_pass:
                break

        return {
            "deleted": deleted,
            "kept_referenced": kept_referenced,
            "busy_with_active_download": sorted(set(busy)),
            "failed": failed,
            "queue_remaining": self.gc_queue_size(),
        }

    def gc_queue_size(self) -> int:
        conn = self._conn()
        try:
            return conn.execute("SELECT COUNT(*) AS n FROM gc_queue").fetchone()["n"]
        finally:
            conn.close()

    def active_reader_count(self) -> int:
        with self._readers_lock:
            return len(self._active_readers)

    async def _record_usage_safely(self, ticket: DownloadTicket) -> None:
        """流末尾入账。

        - 放到工作线程执行，避免 SQLite 写入阻塞事件循环；
        - 用 shield 保护：文件已读到 EOF 且所有块都已交给 ASGI 服务器后，
          即便客户端恰好在此刻断开（响应任务被取消）也要把这一笔写完；
        - 失败只记录日志，不向客户端抛出（数据已发完）。
        """
        try:
            with anyio.CancelScope(shield=True):
                await anyio.to_thread.run_sync(self.record_completed_download, ticket)
        except Exception:  # pragma: no cover - 故障注入测试之外不应触发
            logger.exception(
                "记录快照用量失败 snapshot_id=%s partition_id=%s",
                ticket.snapshot_id,
                ticket.partition_id,
            )

    def stream_blob(self, path: Path, ticket: DownloadTicket, chunk_size: int = 1024 * 1024):
        """构造受租约保护的异步文件流，并在正常读完后记一次用量。

        使用 async 生成器而非同步生成器：Starlette 对同步迭代器会用
        ``iterate_in_threadpool`` 临时包装，内层生成器在两次取值之间可能被
        GC，导致 finally 提前释放租约。async 生成器会被响应迭代器强引用
        到流结束；这里再用 ``_active_streams`` 集合保留一份强引用做双保险。

        “完整发送”的判定：只有文件被读到 EOF、且生成器在最后一次 ``yield``
        之后又向前推进一步（即最后一字节已被 ASGI 服务器接受）才入账。
        客户端在首块前或中途断开时生成器在 ``yield`` 处被关闭，永远不会走到
        记账语句；异常退出同样不入账。
        """
        async def generator():
            stream_token = object()
            self._active_streams.add(stream_token)
            try:
                async with await anyio.open_file(path, "rb") as fh:
                    while True:
                        chunk = await fh.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk
                # 已读到 EOF 且所有块都已交给 ASGI 服务器：本次下载完成
                await self._record_usage_safely(ticket)
            finally:
                self._active_streams.discard(stream_token)
                self.release_download(ticket.lease_id)

        return generator()
