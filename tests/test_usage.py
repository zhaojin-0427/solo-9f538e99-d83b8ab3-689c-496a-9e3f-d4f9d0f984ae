"""快照用量账本：完整发送计数、并发分别计数、释放/回收后可查、失败不留半笔。"""
import threading
import time
from pathlib import Path

from conftest import make_csv, upload

from app.service import DownloadTicket


def _usage(client_or_http, snapshot_id: int):
    r = client_or_http.get(f"/snapshots/{snapshot_id}/usage")
    assert r.status_code == 200, r.text
    return r.json()


def _by_partition(body):
    return {p["partition_id"]: p for p in body["partitions"]}


def _wait_for(predicate, timeout=15.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_usage_lists_all_partitions_even_if_never_downloaded(client):
    upload(client, "a", make_csv([("A", 0.1)]))
    upload(client, "b", make_csv([("B", 0.2)]))
    sid = client.post("/snapshots").json()["snapshot_id"]

    body = _usage(client, sid)
    assert body["snapshot_id"] == sid
    assert body["released_at"] is None
    assert body["partition_count"] == 2
    parts = _by_partition(body)
    assert set(parts) == {"a", "b"}
    for p in parts.values():
        assert len(p["sha256"]) == 64
        assert p["completed_downloads"] == 0
        assert p["total_bytes"] == 0
        assert p["last_completed_at"] is None

    assert client.get("/snapshots/9999/usage").status_code == 404


def test_completed_downloads_counted_per_partition_and_accumulate(client):
    csv_a = make_csv([("A", 0.1), ("A2", 0.2)])
    csv_b = make_csv([("B", 0.3)])
    sha_a = upload(client, "a", csv_a).json()["sha256"]
    upload(client, "b", csv_b)
    sid = client.post("/snapshots").json()["snapshot_id"]

    for _ in range(3):
        r = client.get(f"/partitions/a/download?snapshot_id={sid}")
        assert r.status_code == 200 and r.content == csv_a
    # 快照内下载 b 一次
    assert client.get(f"/partitions/b/download?snapshot_id={sid}").content == csv_b
    # 当前分区下载不属于快照用量
    assert client.get("/partitions/a/download").content == csv_a

    parts = _by_partition(_usage(client, sid))
    assert parts["a"]["sha256"] == sha_a
    assert parts["a"]["completed_downloads"] == 3
    assert parts["a"]["total_bytes"] == 3 * len(csv_a)
    assert parts["a"]["last_completed_at"] is not None
    assert parts["b"]["completed_downloads"] == 1
    assert parts["b"]["total_bytes"] == len(csv_b)


def test_usage_survives_release_and_blob_reclaim(client, data_dir: Path):
    csv_v1 = make_csv([("S1", 0.1)])
    sha_v1 = upload(client, "day", csv_v1).json()["sha256"]
    sid = client.post("/snapshots").json()["snapshot_id"]
    assert client.get(f"/partitions/day/download?snapshot_id={sid}").content == csv_v1

    # 固定后替换分区：快照仍读旧字节，账本中的 SHA 也应保持固定时的值
    csv_v2 = make_csv([("S1", 0.9)])
    sha_v2 = upload(client, "day", csv_v2).json()["sha256"]
    assert sha_v2 != sha_v1
    assert client.get(f"/partitions/day/download?snapshot_id={sid}").content == csv_v1
    parts = _by_partition(_usage(client, sid))
    assert parts["day"]["sha256"] == sha_v1
    assert parts["day"]["completed_downloads"] == 2
    assert parts["day"]["blob_retained"] is True

    # 释放快照并回收原件：历史用量仍可查，新读取被拒
    assert client.post(f"/snapshots/{sid}/release").status_code == 200
    gc = client.post("/gc").json()
    assert sha_v1 in gc["deleted"]
    assert client.get(f"/partitions/day/download?snapshot_id={sid}").status_code == 409

    body = _usage(client, sid)
    assert body["released_at"] is not None
    parts = _by_partition(body)
    assert parts["day"]["sha256"] == sha_v1
    assert parts["day"]["completed_downloads"] == 2
    assert parts["day"]["total_bytes"] == 2 * len(csv_v1)
    assert parts["day"]["blob_retained"] is False

    # 之后的新快照是独立账本行，不受旧快照历史影响
    sid2 = client.post("/snapshots").json()["snapshot_id"]
    parts2 = _by_partition(_usage(client, sid2))
    assert parts2["day"]["completed_downloads"] == 0
    assert parts2["day"]["sha256"] == sha_v2


def _make_big_csv(rows: int = 600000) -> bytes:
    data = make_csv([(f"S{i:07d}", (i % 1000) / 1000.0) for i in range(rows)])
    assert len(data) > 8 * 1024 * 1024  # 远大于 TCP 发送缓冲，便于制造反压
    return data


def test_aborted_downloads_are_not_counted(live_server):
    http, svc, _base = live_server
    csv_big = _make_big_csv()
    upload(http, "big", csv_big)
    sid = http.post("/snapshots").json()["snapshot_id"]

    # 读到首块后立刻断开（中途断开）
    with http.stream(
        "GET", "/partitions/big/download", params={"snapshot_id": sid}
    ) as resp:
        it = resp.iter_bytes(chunk_size=64)
        first = next(it)
        assert first
        assert svc.active_reader_count() == 1
    deadline = time.time() + 10
    while svc.active_reader_count() and time.time() < deadline:
        time.sleep(0.05)
    assert svc.active_reader_count() == 0

    # 首块前断开（收到响应头即关闭）
    with http.stream(
        "GET", "/partitions/big/download", params={"snapshot_id": sid}
    ) as resp:
        assert resp.status_code == 200
        _ = resp.headers  # 触发响应建立但不读任何 body

    time.sleep(0.5)
    parts = _by_partition(_usage(http, sid))
    assert parts["big"]["completed_downloads"] == 0
    assert parts["big"]["total_bytes"] == 0
    assert parts["big"]["last_completed_at"] is None

    # 完整读完才计一次
    r = http.get("/partitions/big/download", params={"snapshot_id": sid})
    assert r.status_code == 200 and r.content == csv_big
    parts = _by_partition(_usage(http, sid))
    assert parts["big"]["completed_downloads"] == 1
    assert parts["big"]["total_bytes"] == len(csv_big)


def test_concurrent_downloads_are_counted_separately(live_server):
    http, _svc, base = live_server
    csv_a = make_csv([(f"S{i:06d}", 0.5) for i in range(150000)])  # 约 2 MiB
    upload(http, "a", csv_a)
    sid = http.post("/snapshots").json()["snapshot_id"]

    n = 8
    barrier = threading.Barrier(n)
    errors = []

    def worker():
        try:
            barrier.wait()
            r = http.get(f"{base}/partitions/a/download", params={"snapshot_id": sid})
            assert r.status_code == 200 and r.content == csv_a
        except Exception as exc:  # pragma: no cover - 失败路径
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
        assert not t.is_alive()
    assert errors == []

    # 客户端读完时服务端可能仍在收尾入账，轮询等待提交
    assert _wait_for(
        lambda: _by_partition(_usage(http, sid))["a"]["completed_downloads"] == n
    ), _usage(http, sid)
    parts = _by_partition(_usage(http, sid))
    assert parts["a"]["completed_downloads"] == n
    assert parts["a"]["total_bytes"] == n * len(csv_a)


def test_download_accepted_before_release_still_counted_after_release(live_server):
    http, svc, _base = live_server
    csv_big = _make_big_csv()
    sha_big = upload(http, "big", csv_big).json()["sha256"]
    sid = http.post("/snapshots").json()["snapshot_id"]

    with http.stream(
        "GET", "/partitions/big/download", params={"snapshot_id": sid}
    ) as resp:
        it = resp.iter_bytes(chunk_size=64)
        first = next(it)
        assert first
        assert svc.active_reader_count() == 1
        # 下载中释放快照：新读取被拒，但已受理的下载继续
        assert http.post(f"/snapshots/{sid}/release").status_code == 200
        assert http.get(
            "/partitions/big/download", params={"snapshot_id": sid}
        ).status_code == 409
        rest = b"".join(it)
        assert first + rest == csv_big

    # 已受理的下载读完后仍入账（服务端收尾可能略晚于客户端读完）
    assert _wait_for(
        lambda: _by_partition(_usage(http, sid))["big"]["completed_downloads"] == 1
    ), _usage(http, sid)
    parts = _by_partition(_usage(http, sid))
    assert parts["big"]["completed_downloads"] == 1
    assert parts["big"]["total_bytes"] == len(csv_big)
    assert parts["big"]["sha256"] == sha_big

    # 随后替换分区使原件成为孤儿并回收，历史仍可查
    csv_new = make_csv([("S1", 0.42)])
    sha_new = upload(http, "big", csv_new).json()["sha256"]
    assert sha_new != sha_big
    gc = http.post("/gc").json()
    assert sha_big in gc["deleted"]
    parts = _by_partition(_usage(http, sid))
    assert parts["big"]["completed_downloads"] == 1
    assert parts["big"]["sha256"] == sha_big
    assert parts["big"]["blob_retained"] is False


def test_usage_write_failure_leaves_no_partial_record(client, svc):
    csv1 = make_csv([("S1", 0.1)])
    upload(client, "day", csv1)
    sid = client.post("/snapshots").json()["snapshot_id"]
    assert client.get(f"/partitions/day/download?snapshot_id={sid}").status_code == 200

    ticket = DownloadTicket(
        snapshot_id=sid, partition_id="day", sha="0" * 64, size=len(csv1),
        path=Path("/nonexistent"), lease_id="test-ticket",
    )
    db = svc._conn()
    try:
        db.execute(
            "CREATE TRIGGER fail_usage AFTER UPDATE ON snapshot_usage"
            " BEGIN SELECT RAISE(ABORT, 'simulated write failure'); END;"
        )
    finally:
        db.close()

    try:
        import pytest

        with pytest.raises(Exception):
            svc.record_completed_download(ticket)
    finally:
        db = svc._conn()
        try:
            db.execute("DROP TRIGGER fail_usage")
        finally:
            db.close()

    # 失败后计数、字节、时间都保持原值（无半笔记录）
    parts = _by_partition(_usage(client, sid))
    assert parts["day"]["completed_downloads"] == 1
    assert parts["day"]["total_bytes"] == len(csv1)

    # 故障消除后可正常入账
    assert svc.record_completed_download(ticket) is True
    parts = _by_partition(_usage(client, sid))
    assert parts["day"]["completed_downloads"] == 2
    assert parts["day"]["total_bytes"] == 2 * len(csv1)


def test_accounting_failure_does_not_break_completed_response(client, svc, monkeypatch):
    csv_a = make_csv([("S1", 0.1)])
    upload(client, "a", csv_a)
    sid = client.post("/snapshots").json()["snapshot_id"]

    def boom(_ticket):
        raise RuntimeError("simulated accounting failure")

    monkeypatch.setattr(svc, "record_completed_download", boom)
    r = client.get(f"/partitions/a/download?snapshot_id={sid}")
    # 数据已完整发送，响应不受记账失败影响
    assert r.status_code == 200 and r.content == csv_a

    monkeypatch.undo()
    parts = _by_partition(_usage(client, sid))
    assert parts["a"]["completed_downloads"] == 0
    assert parts["a"]["total_bytes"] == 0


def test_restart_keeps_committed_stats_and_does_not_backfill_unfinished(data_dir: Path):
    from app.config import Settings
    from app.service import MoistureService
    import app.main as main
    from fastapi.testclient import TestClient

    csv1 = make_csv([("S1", 0.1)])
    svc1 = MoistureService(Settings(data_dir=data_dir))
    main.service = svc1
    with TestClient(main.app) as c:
        upload(c, "day", csv1)
        sid = c.post("/snapshots").json()["snapshot_id"]
        assert c.get(f"/partitions/day/download?snapshot_id={sid}").status_code == 200
        # 受理但未读完的下载：取得票据/租约后“进程中断”
        ticket = svc1.open_partition_download("day", sid)
        assert ticket.snapshot_id == sid
        before = c.get(f"/snapshots/{sid}/usage").json()

    # 重启：已提交统计保留；未完成请求不补记
    svc2 = MoistureService(Settings(data_dir=data_dir))
    svc2.startup()
    main.service = svc2
    with TestClient(main.app) as c:
        after = c.get(f"/snapshots/{sid}/usage").json()
        assert after["partitions"] == before["partitions"]
        part = _by_partition(after)["day"]
        assert part["completed_downloads"] == 1
        assert part["total_bytes"] == len(csv1)
        assert part["sha256"] == ticket.sha
        # 重启清空遗留租约，文件可正常回收
        assert svc2.active_reader_count() == 0
        assert c.get(f"/partitions/day/download?snapshot_id={sid}").status_code == 200


def test_init_db_backfills_legacy_snapshot_usage_rows_without_guessing(data_dir: Path):
    """升级前已存在的快照：补建零值账本行，不凭空补记未完成下载。"""
    from app.config import Settings
    from app.db import init_db
    from app.service import MoistureService
    import app.main as main
    from fastapi.testclient import TestClient

    svc = MoistureService(Settings(data_dir=data_dir))
    main.service = svc
    with TestClient(main.app) as c:
        upload(c, "a", make_csv([("A", 0.1)]))
        upload(c, "b", make_csv([("B", 0.2)]))
        sid = c.post("/snapshots").json()["snapshot_id"]
        c.get(f"/partitions/a/download?snapshot_id={sid}")
        assert _by_partition(c.get(f"/snapshots/{sid}/usage").json())["a"][
            "completed_downloads"
        ] == 1

        # 模拟旧库：账本表为空（升级前创建的快照）
        db = svc._conn()
        try:
            db.execute("DELETE FROM snapshot_usage")
        finally:
            db.close()

        init_db(svc.cfg)
        parts = _by_partition(c.get(f"/snapshots/{sid}/usage").json())
        assert set(parts) == {"a", "b"}
        assert all(p["completed_downloads"] == 0 for p in parts.values())
        assert all(p["total_bytes"] == 0 for p in parts.values())
