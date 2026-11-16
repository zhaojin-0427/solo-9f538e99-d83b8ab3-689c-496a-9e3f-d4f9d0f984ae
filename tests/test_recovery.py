"""崩溃恢复：写盘/登记失败不改原分区；重启后继续回收且引用完好。"""
import sqlite3
from pathlib import Path

from conftest import make_csv, upload


def test_registration_failure_keeps_old_partition(client, data_dir: Path, monkeypatch):
    csv_v1 = make_csv([("S1", 0.1)])
    csv_v2 = make_csv([("S1", 0.2)])
    upload(client, "day", csv_v1)

    from app import storage

    # 第一次成功上传之后安装故障注入：此后任何 publish_blob 都失败，
    # 模拟第二次上传时写盘失败
    real_publish = storage.publish_blob

    def flaky_publish(tmp_path, sha, cfg):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(storage, "publish_blob", flaky_publish)
    r = upload(client, "day", csv_v2)
    assert r.status_code == 500
    monkeypatch.setattr(storage, "publish_blob", real_publish)

    # 原分区未变、完整可读；临时文件已被清理
    assert client.get("/partitions/day/download").content == csv_v1
    leftovers = list((data_dir / "tmp").iterdir())
    assert leftovers == []


def test_restart_resumes_gc_and_preserves_references(client, data_dir: Path, svc):
    csv_v1 = make_csv([("S1", 0.1)])
    csv_v2 = make_csv([("S1", 0.2)])
    sha_v1 = upload(client, "day", csv_v1).json()["sha256"]
    # 固定快照保护 v1，再把当前分区替换成 v2（v1 仍被快照引用，不会入队）
    snap_id = client.post("/snapshots").json()["snapshot_id"]
    upload(client, "day", csv_v2)
    assert client.post("/gc").json()["deleted"] == []

    # 模拟“服务中断”：直接把 v1 放入回收队列（如同删除阶段中途崩溃）
    db = svc._conn()
    try:
        db.execute("BEGIN IMMEDIATE;")
        db.execute(
            "INSERT INTO gc_queue(sha256, enqueued_at, attempts) VALUES (?, ?, 0)",
            (sha_v1, "2026-01-01T00:00:00+00:00"),
        )
        db.commit()
    finally:
        db.close()

    # 重启：新服务实例对同一数据目录执行 startup 恢复
    from app.config import Settings
    from app.service import MoistureService

    restarted = MoistureService(Settings(data_dir=data_dir))
    restarted.startup()

    # v1 仍被未释放快照引用：恢复回收时必须保留且出队
    assert restarted.gc_queue_size() == 0
    r = client.get(f"/partitions/day/download?snapshot_id={snap_id}")
    assert r.status_code == 200 and r.content == csv_v1
    # 当前分区 v2 完整可读
    assert client.get("/partitions/day/download").content == csv_v2

    # 释放快照后重启恢复，v1 才被真正回收
    client.post(f"/snapshots/{snap_id}/release")
    # 再入队模拟“释放与删除之间服务中断”（先清掉可能已存在的队列项）
    db = svc._conn()
    try:
        db.execute("BEGIN IMMEDIATE;")
        db.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha_v1,))
        db.execute(
            "INSERT INTO gc_queue(sha256, enqueued_at, attempts) VALUES (?, ?, 0)",
            (sha_v1, "2026-01-02T00:00:00+00:00"),
        )
        db.commit()
    finally:
        db.close()

    restarted2 = MoistureService(Settings(data_dir=data_dir))
    restarted2.startup()  # startup 内部会跑一轮 gc
    remaining = [p.name for p in (data_dir / "blobs").rglob("*") if p.is_file()]
    assert sha_v1 not in remaining
    assert restarted2.gc_queue_size() == 0
    # 当前分区 v2 依然完好
    assert client.get("/partitions/day/download").content == csv_v2


def test_orphan_disk_file_is_reconciled_on_restart(data_dir: Path):
    from app.config import Settings
    from app.service import MoistureService

    svc = MoistureService(Settings(data_dir=data_dir))
    svc.startup()
    # 数据卷上出现一个无登记文件（模拟上次中断遗留）
    from app import storage

    orphan = "a" * 64
    p = storage.blob_path(orphan, svc.cfg)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"leftover")

    restarted = MoistureService(Settings(data_dir=data_dir))
    rec = restarted.reconcile_disk()
    assert rec["orphan_files_enqueued"] == 1
    gc = restarted.run_gc()
    assert orphan in gc["deleted"]
    assert not p.exists()


def test_sqlite_foreign_keys_and_wal_enabled(data_dir: Path):
    from app.config import Settings
    from app.db import connect, init_db

    cfg = Settings(data_dir=data_dir)
    init_db(cfg)
    conn = connect(cfg)
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        # 外键约束生效：不能插入指向不存在 blob 的分区条目
        conn.execute("BEGIN IMMEDIATE;")
        try:
            conn.execute(
                "INSERT INTO blobs(sha256, size, created_at) VALUES (?, 1, 'x')",
                ("b" * 64,),
            )
            conn.execute(
                "INSERT INTO partition_entries(partition_id, row_index, station,"
                " moisture, blob_sha) VALUES ('p', 0, 's', 0.1, ?)",
                ("c" * 64,),
            )
            raise AssertionError("外键约束应当拦截无效引用")
        except sqlite3.IntegrityError:
            pass
        finally:
            conn.rollback()
    finally:
        conn.close()
