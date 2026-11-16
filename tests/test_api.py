"""核心功能：上传/替换、SHA-256 去重、快照固定/释放、回收规则。"""
from pathlib import Path

from conftest import make_csv, upload


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_upload_then_download_roundtrip(client):
    csv1 = make_csv([("S1", 0.12), ("S2", 0.34)])
    r = upload(client, "2026-11-15", csv1)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["row_count"] == 2
    assert body["replaced"] is False
    assert len(body["sha256"]) == 64

    r = client.get("/partitions/2026-11-15/download")
    assert r.status_code == 200
    assert r.content == csv1
    assert r.headers["X-Blob-SHA256"] == body["sha256"]


def test_identical_bytes_share_one_blob_file(client, data_dir: Path):
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    a = upload(client, "2026-11-15", csv1).json()["sha256"]
    b = upload(client, "2026-11-16", csv1).json()["sha256"]
    assert a == b

    blobs = list((data_dir / "blobs").rglob("*"))
    files = [p for p in blobs if p.is_file()]
    assert len(files) == 1  # 相同字节只有一个数据卷文件


def test_replacement_keeps_old_version_via_snapshot_and_gcs_unreferenced(client, data_dir: Path):
    csv_v1 = make_csv([("S1", 0.1)])
    csv_v2 = make_csv([("S1", 0.9)])
    sha_v1 = upload(client, "2026-11-15", csv_v1).json()["sha256"]

    # 先固定快照：v1 被快照引用
    snap = client.post("/snapshots").json()
    snap_id = snap["snapshot_id"]

    # 再整份替换成 v2
    upload(client, "2026-11-15", csv_v2)
    sha_v2 = client.get("/partitions/2026-11-15").json()["current_blob"]
    assert sha_v2 != sha_v1

    # 启动时已自动回收过；再触发一次：v1 受快照保护不能删，v2 是当前分区也不能删
    r = client.post("/gc")
    assert r.status_code == 200
    gc = r.json()
    assert sha_v1 not in gc["deleted"]
    assert sha_v2 not in gc["deleted"]

    # 快照里读到的仍是旧内容
    r = client.get(f"/partitions/2026-11-15/download?snapshot_id={snap_id}")
    assert r.status_code == 200
    assert r.content == csv_v1
    # 当前分区已是新内容（更新分区不改变已固定快照）
    assert client.get("/partitions/2026-11-15/download").content == csv_v2

    # 再替换一次，使 v2 成为旧版本且无快照引用
    csv_v3 = make_csv([("S1", 0.55)])
    sha_v3 = upload(client, "2026-11-15", csv_v3).json()
    # v2 无任何快照引用，应被回收；v1 仍受快照保护
    gc = client.post("/gc").json()
    assert sha_v2 in gc["deleted"]
    assert sha_v1 not in gc["deleted"]
    assert sha_v3 not in gc["deleted"]  # 当前分区

    # 释放快照后 v1 才可回收
    r = client.post(f"/snapshots/{snap_id}/release")
    assert r.status_code == 200
    gc = client.post("/gc").json()
    assert sha_v1 in gc["deleted"]
    assert client.get("/gc/status").json()["queue_remaining"] == 0


def test_released_snapshot_refuses_new_reads(client):
    upload(client, "2026-11-15", make_csv([("S1", 0.1)]))
    snap_id = client.post("/snapshots").json()["snapshot_id"]
    assert client.post(f"/snapshots/{snap_id}/release").status_code == 200

    r = client.get(f"/partitions/2026-11-15/download?snapshot_id={snap_id}")
    assert r.status_code == 409

    # 重复释放也是 409
    assert client.post(f"/snapshots/{snap_id}/release").status_code == 409
    # 不存在的快照
    assert client.post("/snapshots/9999/release").status_code == 404


def test_snapshot_pins_all_partitions_and_survives_replacement(client):
    csv_a = make_csv([("A", 0.1)])
    csv_b = make_csv([("B", 0.2)])
    upload(client, "day-1", csv_a)
    upload(client, "day-2", csv_b)
    snap = client.post("/snapshots").json()
    assert snap["partition_count"] == 2
    snap_id = snap["snapshot_id"]

    # 全部替换后，快照号仍能读到旧字节
    upload(client, "day-1", make_csv([("A", 0.7)]))
    upload(client, "day-2", make_csv([("B", 0.8)]))
    assert client.get(f"/partitions/day-1/download?snapshot_id={snap_id}").content == csv_a
    assert client.get(f"/partitions/day-2/download?snapshot_id={snap_id}").content == csv_b

    gc = client.post("/gc").json()
    assert gc["deleted"] == []  # 旧版本全部被未释放快照引用


def test_invalid_csv_does_not_change_existing_partition(client):
    csv_ok = make_csv([("S1", 0.1)])
    sha_ok = upload(client, "2026-11-15", csv_ok).json()["sha256"]

    bad = b"\xe4\xb8\xad\xff not a valid utf8 csv"
    r = upload(client, "2026-11-15", bad)
    assert r.status_code == 422

    missing_col = "station,value\nS1,0.5\n".encode()
    r = upload(client, "2026-11-15", missing_col)
    assert r.status_code == 422

    bad_number = "站点,湿度\nS1,abc\n".encode()
    r = upload(client, "2026-11-15", bad_number)
    assert r.status_code == 422

    # 原分区完整可读
    assert client.get("/partitions/2026-11-15/download").content == csv_ok
    assert client.get("/partitions/2026-11-15").json()["current_blob"] == sha_ok


def test_reupload_same_content_after_gc_still_works(client):
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    sha = upload(client, "2026-11-15", csv1).json()["sha256"]
    # 替换成别的内容，旧 blob 无快照引用 -> 被回收，文件已删除
    upload(client, "2026-11-15", make_csv([("S1", 0.3)]))
    gc = client.post("/gc").json()
    assert sha in gc["deleted"]

    # 同内容重新上传：重新发布文件并可正常下载
    r = upload(client, "2026-11-15", csv1)
    assert r.status_code == 200
    assert r.json()["sha256"] == sha
    assert client.get("/partitions/2026-11-15/download").content == csv1


def test_english_headers_and_blank_lines_accepted(client):
    content = b"station, moisture\nS1, 0.1\n\nS2,0.2\n"
    r = upload(client, "2026-11-15", content)
    assert r.status_code == 200
    assert r.json()["row_count"] == 2


def test_partition_listing_and_404(client):
    assert client.get("/partitions").json()["partitions"] == []
    assert client.get("/partitions/nope").status_code == 404
    assert client.get("/partitions/nope/download").status_code == 404
    upload(client, "2026-11-15", make_csv([("S1", 0.1)]))
    assert len(client.get("/partitions").json()["partitions"]) == 1
