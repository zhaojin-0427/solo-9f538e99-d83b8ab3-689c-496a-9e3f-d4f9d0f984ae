"""慢下载与上传/释放/回收交错时的安全性（真实 uvicorn + httpx 端到端）。"""
import threading

import httpx

from conftest import make_csv, upload


def test_slow_download_protects_file_and_does_not_block_other_operations(live_server):
    http, svc, base = live_server

    # 大文件（约 8 MiB，全是合法行）：慢客户端只逐字节读取，TCP 发送缓冲
    # 被填满后，服务端流式任务会因反压而挂起，租约在整个下载期间持续有效。
    # 小文件会被一次性写入 OS 缓冲、服务端任务提前结束，无法模拟“慢下载”。
    csv_a = make_csv([(f"S{i:07d}", (i % 1000) / 1000.0) for i in range(600000)])
    assert len(csv_a) > 8 * 1024 * 1024  # 约 9 MiB，远大于 TCP 发送缓冲
    csv_b = make_csv([("S1", 0.2)])
    sha_a = upload(http, "day", csv_a).json()["sha256"]

    errors = []

    def slow_download():
        try:
            with httpx.stream("GET", f"{base}/partitions/day/download", timeout=60) as resp:
                assert resp.status_code == 200
                # 先读一个小块触发服务端响应建立；大文件随后产生 TCP 反压，
                # 服务端流式任务挂起，租约在整个慢下载期间保持有效。
                it = resp.iter_bytes(chunk_size=64)
                first = next(it)
                assert first
                # 确认服务端租约已建立且在持续
                assert svc.active_reader_count() == 1

                # 下载进行中替换分区：旧 blob 进入回收队列
                sha_b = upload(http, "day", csv_b).json()["sha256"]
                assert sha_a != sha_b

                # 回收时带租约的旧文件必须保留并标记 busy，不能删除
                gc = http.post("/gc").json()
                assert sha_a in gc["busy_with_active_download"]
                assert sha_a not in gc["deleted"]
                assert svc.active_reader_count() == 1

                # 慢下载期间，其他文件的回收照常完成（不被慢下载阻塞）
                csv_c = make_csv([("S9", 0.9)])
                sha_c = upload(http, "other", csv_c).json()["sha256"]
                upload(http, "other", make_csv([("S9", 0.1)]))
                gc2 = http.post("/gc").json()
                assert sha_c in gc2["deleted"]

                # 快照固定与释放也能在慢下载期间完成
                snap = http.post("/snapshots").json()["snapshot_id"]
                assert http.post(f"/snapshots/{snap}/release").status_code == 200

                # 关键断言完成后用大块快速排空剩余字节，校验下载完整且为旧版本
                rest = b"".join(it)
                assert first + rest == csv_a
        except Exception as exc:  # pragma: no cover - 测试失败路径
            errors.append(exc)

    t = threading.Thread(target=slow_download)
    t.start()
    t.join(timeout=90)
    assert not t.is_alive(), "慢下载线程超时"
    assert errors == []

    # 下载结束、租约释放后，再回收即可删除旧 blob
    gc = http.post("/gc").json()
    assert sha_a in gc["deleted"]
    assert svc.active_reader_count() == 0


def test_concurrent_uploads_of_same_bytes_share_blob(live_server):
    http, _svc, _base = live_server
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    results = []
    barrier = threading.Barrier(5)

    def worker(pid: str):
        barrier.wait()
        results.append(upload(http, pid, csv1).json()["sha256"])

    threads = [threading.Thread(target=worker, args=(f"p{i}",)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()
    assert len(set(results)) == 1
    status = http.get("/gc/status").json()
    assert status["blob_files_on_disk"] == 1


def test_concurrent_upload_release_gc_never_breaks_snapshot_read(live_server):
    http, _svc, base = live_server
    upload(http, "a", make_csv([("A", 0.1)]))
    upload(http, "b", make_csv([("B", 0.2)]))
    snap_id = http.post("/snapshots").json()["snapshot_id"]
    expected = http.get(
        f"{base}/partitions/a/download", params={"snapshot_id": snap_id}
    ).content

    stop = threading.Event()
    errors = []

    def writer():
        i = 0
        while not stop.is_set():
            try:
                upload(http, "a", make_csv([("A", round(0.01 * (i % 50), 4))]))
                http.post("/gc")
                i += 1
            except Exception as exc:  # pragma: no cover
                errors.append(exc)
                stop.set()

    def reader():
        while not stop.is_set():
            try:
                r = http.get(
                    f"{base}/partitions/a/download",
                    params={"snapshot_id": snap_id},
                )
                assert r.status_code == 200 and r.content == expected
            except Exception as exc:  # pragma: no cover
                errors.append(exc)
                stop.set()

    tw = threading.Thread(target=writer)
    tr = threading.Thread(target=reader)
    tw.start()
    tr.start()
    stop.wait(5)
    stop.set()
    tw.join(timeout=30)
    tr.join(timeout=30)
    assert errors == []

    # 释放后才允许拒绝新读取
    assert http.post(f"/snapshots/{snap_id}/release").status_code == 200
    r = http.get(
        f"{base}/partitions/a/download", params={"snapshot_id": snap_id}
    )
    assert r.status_code == 409
    http.post("/gc")


def test_released_snapshot_refuses_and_current_still_readable(live_server):
    http, _svc, base = live_server
    upload(http, "day", make_csv([("S1", 0.42)]))
    snap = http.post("/snapshots").json()["snapshot_id"]
    assert http.post(f"/snapshots/{snap}/release").status_code == 200
    r = http.get(f"{base}/partitions/day/download", params={"snapshot_id": snap})
    assert r.status_code == 409
    # 当前分区不受影响
    assert http.get(f"{base}/partitions/day/download").status_code == 200
