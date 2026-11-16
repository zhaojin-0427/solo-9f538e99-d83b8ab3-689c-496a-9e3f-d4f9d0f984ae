"""FastAPI 入口：分区上传/下载、快照固定/释放、回收触发与状态查看。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, File, HTTPException, Path as PathParam, Query, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from . import storage
from .config import settings
from .service import (
    CsvValidationError,
    MissingBlobFile,
    MoistureService,
    NotFound,
    ServiceError,
    SnapshotReleased,
)
from .storage import UploadTooLarge

service = MoistureService(settings)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 建表 + 崩溃恢复：清理临时文件、对账数据卷、继续未完成的回收
    service.startup()
    yield


app = FastAPI(
    title="土壤湿度分区内容寻址存储 API",
    version="1.0.0",
    description=(
        "按日期分区上传 CSV（站点、湿度），整份替换当前分区；"
        "相同字节按 SHA-256 共用数据卷文件；支持快照固定/释放与安全回收。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(ServiceError)
def _service_error_handler(_request, exc: ServiceError):
    status = 400
    if isinstance(exc, NotFound):
        status = 404
    elif isinstance(exc, SnapshotReleased):
        status = 409
    elif isinstance(exc, MissingBlobFile):
        status = 500
    return JSONResponse(status_code=status, content={"error": str(exc)})


@app.exception_handler(UploadTooLarge)
def _too_large_handler(_request, exc: UploadTooLarge):
    return JSONResponse(status_code=413, content={"error": str(exc)})


@app.get("/health")
def health():
    return {"status": "ok", "data_dir": str(service.cfg.data_dir)}


# --------------------------------------------------------------------------- #
# 分区
# --------------------------------------------------------------------------- #
@app.post("/partitions/{partition_id}", status_code=200)
async def upload_partition(
    partition_id: str = PathParam(..., description="日期分区标识，如 2026-11-15"),
    file: UploadFile = File(..., description="CSV 文件，含站点、湿度两列"),
):
    """上传 CSV，整份替换该日期分区的当前版本。"""
    try:
        result = service.upload_partition(partition_id, file.file)
    except CsvValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except UploadTooLarge as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    return result


@app.get("/partitions")
def list_partitions():
    return {"partitions": service.list_partitions()}


@app.get("/partitions/{partition_id}")
def get_partition(partition_id: str):
    try:
        return service.get_partition(partition_id)
    except NotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.get("/partitions/{partition_id}/download")
def download_partition(
    partition_id: str,
    snapshot_id: Optional[int] = Query(
        default=None, description="快照号；提供时读取该快照固定的版本，释放后拒绝读取"
    ),
):
    """下载分区 CSV。默认读当前分区；带 snapshot_id 读快照固定版本。

    下载开始即取得租约，流式传输期间不持元数据锁，慢下载不阻塞
    上传/释放/其他文件回收，且本文件在下载结束前不会被回收。
    """
    try:
        sha, path, cleanup = service.open_partition_download(partition_id, snapshot_id)
    except NotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SnapshotReleased as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except MissingBlobFile as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    stream = service.stream_blob(path, cleanup)
    filename = f"{partition_id}.csv"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "X-Blob-SHA256": sha,
    }
    if snapshot_id is not None:
        headers["X-Snapshot-ID"] = str(snapshot_id)
    return StreamingResponse(stream, media_type="text/csv", headers=headers)


# --------------------------------------------------------------------------- #
# 快照
# --------------------------------------------------------------------------- #
@app.post("/snapshots", status_code=201)
def create_snapshot():
    """固定当前全部已提交分区，返回快照号（作业用它读取不变视图）。"""
    return service.create_snapshot()


@app.get("/snapshots")
def list_snapshots():
    return {"snapshots": service.list_snapshots()}


@app.post("/snapshots/{snapshot_id}/release")
def release_snapshot(snapshot_id: int):
    """释放快照；释放后拒绝新读取，独占的 blob 进入回收流程。"""
    try:
        return service.release_snapshot(snapshot_id)
    except NotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except SnapshotReleased as exc:
        raise HTTPException(status_code=409, detail=str(exc))


# --------------------------------------------------------------------------- #
# 回收
# --------------------------------------------------------------------------- #
@app.post("/gc")
def run_gc():
    """立即执行一轮回收（服务启动时也会自动执行）。"""
    return service.run_gc()


@app.get("/gc/status")
def gc_status():
    return {
        "queue_remaining": service.gc_queue_size(),
        "active_downloads": service.active_reader_count(),
        "blob_files_on_disk": len(storage.list_blob_files(service.cfg)),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        workers=1,
    )
