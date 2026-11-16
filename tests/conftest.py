"""pytest 公共夹具：每个用例独立临时数据目录 + FastAPI TestClient。

通过依赖覆盖把服务指向临时 DATA_DIR，避免模块级单例污染。
"""
import socket
import sys
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

# 保证可以 import app（从仓库根运行 pytest）
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings  # noqa: E402
from app.service import MoistureService  # noqa: E402
import app.main as main  # noqa: E402


@pytest.fixture()
def data_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    return d


@pytest.fixture()
def svc(data_dir: Path) -> MoistureService:
    return MoistureService(Settings(data_dir=data_dir))


@pytest.fixture()
def client(svc: MoistureService) -> TestClient:
    main.service = svc  # 类型: ignore[attr-defined]
    # raise_server_exceptions=False：让服务端异常走正常的 500 响应路径，
    # 便于断言 HTTP 状态码
    with TestClient(main.app, raise_server_exceptions=False) as c:
        yield c


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def live_server(data_dir: Path):
    """真实 uvicorn 服务器（随机端口）+ httpx 客户端，用于流式并发测试。"""
    service = MoistureService(Settings(data_dir=data_dir))
    main.service = service
    port = _free_port()
    config = uvicorn.Config(
        main.app, host="127.0.0.1", port=port, log_level="warning", workers=1
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            if httpx.get(f"{base}/health", timeout=1).status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.1)
    else:  # pragma: no cover
        raise RuntimeError("测试服务器未能在限定时间内启动")

    with httpx.Client(base_url=base, timeout=30) as http:
        yield http, service, base

    server.should_exit = True
    thread.join(timeout=10)


def make_csv(rows, header=("站点", "湿度")) -> bytes:
    lines = [",".join(header)]
    for station, moisture in rows:
        lines.append(f"{station},{moisture}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def upload(client, partition_id: str, content: bytes):
    return client.post(
        f"/partitions/{partition_id}",
        files={"file": (f"{partition_id}.csv", content, "text/csv")},
    )
