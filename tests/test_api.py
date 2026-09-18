"""端到端行为测试。

运行: pytest -q （需先 pip install -r requirements-dev.txt）
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path

import pytest


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient

    from app.main import app

    with TestClient(app) as c:
        yield c


def submit(client, tasks, name="dag", dag_id=None):
    body = {"name": name, "tasks": tasks}
    if dag_id:
        body["id"] = dag_id
    r = client.post("/dags", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def by_key(dag, key):
    return next(t for t in dag["tasks"] if t["key"] == key)


# ------------------------------------------------------------- 提交校验

def test_submit_valid_dag(client):
    dag = submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"]},
        ],
    )
    assert dag["task_count"] == 2
    assert dag["states"]["pending"] == 2


def test_submit_rejects_missing_dependency(client):
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["ghost"]}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "missing_dependency"
    assert "ghost" in r.json()["error"]["details"]["missing"]


def test_submit_rejects_self_dependency_cycle(client):
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["a"]}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cycle_detected"


def test_submit_rejects_long_cycle(client):
    r = client.post(
        "/dags",
        json={"tasks": [{"key": "a", "deps": ["c"]}, {"key": "b", "deps": ["a"]}, {"key": "c", "deps": ["b"]}]},
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cycle_detected"
    assert r.json()["error"]["details"]["cycle"][0] == r.json()["error"]["details"]["cycle"][-1]


def test_submit_rejects_duplicate_key(client):
    r = client.post("/dags", json={"tasks": [{"key": "a"}, {"key": "a"}]})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "duplicate_task_key"


# ------------------------------------------------------------- 依赖门控

def test_claim_respects_dependencies(client):
    dag = submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}])
    r = client.post("/tasks/claim", json={"limit": 10})
    assert r.status_code == 200
    keys = {t["task_key"] for t in r.json()["claimed"]}
    assert keys == {"a"}

    # a 已在 running 且租约未过期，不重复领取；b 的前置未完成，也不可领
    r = client.post("/tasks/claim", json={"dag_id": dag["id"]})
    assert r.json()["count"] == 0


def test_full_dependency_flow(client):
    dag = submit(
        client,
        [
            {"key": "a"},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["a"]},
            {"key": "d", "deps": ["b", "c"]},
        ],
    )
    dag_id = dag["id"]

    # 只有 a 可领
    claimed = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert [t["task_key"] for t in claimed] == ["a"]
    token_a = claimed[0]["lease_token"]
    assert claimed[0]["attempt"] == 1

    # 完成 a 后 b、c 同时可领
    assert client.post(f"/tasks/{claimed[0]['task_id']}/complete",
                       json={"completion_id": "cmp-a", "lease_token": token_a, "success": True}).status_code == 200
    claimed = client.post("/tasks/claim", json={"dag_id": dag_id, "limit": 10}).json()["claimed"]
    assert {t["task_key"] for t in claimed} == {"b", "c"}
    for t in claimed:
        assert t["attempt"] == 1

    # d 仍被门控
    assert client.post("/tasks/claim", json={"dag_id": dag_id}).json()["count"] == 0

    for t in claimed:
        client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": f"cmp-{t['task_key']}",
                          "lease_token": t["lease_token"], "success": True})
    d = client.post("/tasks/claim", json={"dag_id": dag_id}).json()["claimed"]
    assert len(d) == 1 and d[0]["task_key"] == "d"


# ------------------------------------------------------------- 尝试与唯一令牌

def test_attempt_increments_and_tokens_unique(client):
    dag = submit(client, [{"key": "a", "lease_seconds": 0.2, "max_retries": 3}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t1["attempt"] == 1
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["task_id"] == t1["task_id"]
    assert t2["attempt"] == 2
    assert t2["lease_token"] != t1["lease_token"]


def test_old_token_operations_return_409_after_reclaim(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    old_token, task_id = t1["lease_token"], t1["task_id"]
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2

    # 旧令牌续租 → 409
    r = client.post(f"/tasks/{task_id}/heartbeat",
                    json={"lease_token": old_token})
    assert r.status_code == 409
    assert r.json()["error"]["code"] in ("invalid_lease", "lease_expired")

    # 旧令牌回报结果 → 409，绝不能覆盖新尝试（completion_id 首次使用也必须先过租约校验）
    r = client.post(f"/tasks/{task_id}/complete",
                    json={"completion_id": "cmp-old", "lease_token": old_token, "success": True})
    assert r.status_code == 409

    status = client.get(f"/tasks/{task_id}").json()
    assert status["state"] == "running"
    assert status["attempt"] == 2
    assert status["lease"]["token"] == t2["lease_token"]


def test_heartbeat_extends_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 0.3}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    time.sleep(0.2)
    r = client.post(f"/tasks/{t['task_id']}/heartbeat",
                    json={"lease_token": t["lease_token"], "extend_seconds": 5})
    assert r.status_code == 200
    time.sleep(0.2)  # 没有心跳的话此刻已过期
    # 租约仍有效：不可再领取，且旧令牌仍可回报
    assert client.post("/tasks/claim", json={}).json()["count"] == 0
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-hb", "lease_token": t["lease_token"], "success": True})
    assert r.status_code == 200


def test_completion_id_is_required(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"lease_token": t["lease_token"], "success": True})
    assert r.status_code == 422


def test_wrong_token_returns_409(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-x",
                          "lease_token": uuid.uuid4().hex, "success": True})
    assert r.status_code == 409


# ------------------------------------------------------------- 幂等回报

def _complete(client, task, cid, **extra):
    body = {"completion_id": cid, "lease_token": task["lease_token"]}
    body.update(extra)
    return client.post(f"/tasks/{task['task_id']}/complete", json=body)


def test_idempotent_replay_returns_identical_response_after_reclaim(client):
    """任务被重新领取后，旧 completion_id 的等价回报仍原样返回首次响应。"""
    submit(client, [{"key": "a", "max_retries": 1, "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]

    payload = {"completion_id": "c1", "lease_token": t1["lease_token"],
               "success": False, "error": "boom"}
    r1 = client.post(f"/tasks/{t1['task_id']}/complete", json=payload)
    assert r1.status_code == 200
    first = r1.json()
    assert first["state"] == "pending" and first["attempt"] == 1
    assert first["failure_count"] == 1 and first["retries_remaining"] == 1

    # 租约过期后被重新领取：新尝试、新令牌
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2 and t2["lease_token"] != t1["lease_token"]

    # 用旧令牌 + 相同 completion_id/内容重放：原样回放，不经过当前租约校验
    r2 = client.post(f"/tasks/{t1['task_id']}/complete", json=payload)
    assert r2.status_code == r1.status_code
    assert r2.text == r1.text  # 逐字节相同的响应体
    assert r2.json() == first

    # 不得再次增加 failure_count，也不影响正在运行的新尝试
    status = client.get(f"/tasks/{t1['task_id']}").json()
    assert status["failure_count"] == 1
    assert status["state"] == "running" and status["attempt"] == 2
    assert status["lease"]["token"] == t2["lease_token"]

    # 新令牌的回报（不同 completion_id）仍走当前租约校验并正常生效
    r3 = _complete(client, t2, "c2", success=True)
    assert r3.status_code == 200 and r3.json()["state"] == "succeeded"
    assert client.get(f"/tasks/{t1['task_id']}").json()["state"] == "succeeded"


def test_idempotent_replay_json_key_order_equivalent(client):
    """result 的 JSON 对象键序不同（含嵌套）视为等价，原样回放首次响应。"""
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]

    r1 = _complete(client, t, "ck", success=True, result={"outer": {"b": 1, "a": [2, 1]}, "k": 0})
    assert r1.status_code == 200
    # 键序调换、空白变化：仍视为同一请求
    r2 = client.post(
        f"/tasks/{t['task_id']}/complete",
        content=json.dumps({"completion_id": "ck", "lease_token": t["lease_token"],
                            "success": True,
                            "result": {"k": 0, "outer": {"a":  [2, 1], "b": 1}}}),
        headers={"Content-Type": "application/json"},
    )
    assert r2.status_code == 200 and r2.text == r1.text
    assert client.get(f"/tasks/{t['task_id']}").json()["state"] == "succeeded"


def test_same_key_different_content_returns_conflict(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r1 = _complete(client, t, "ck", success=True, result={"rows": 1})
    assert r1.status_code == 200

    # success 不同
    r2 = _complete(client, t, "ck", success=False, error="now failed")
    assert r2.status_code == 409
    assert r2.json()["error"]["code"] == "idempotency_conflict"
    assert r2.json()["error"]["details"]["completion_id"] == "ck"

    # 即便租约令牌也无效，只要键存在且内容不同，仍是冲突而非租约错误
    r3 = client.post(
        f"/tasks/{t['task_id']}/complete",
        json={"completion_id": "ck", "lease_token": uuid.uuid4().hex,
              "success": True, "result": {"rows": 2}},
    )
    assert r3.status_code == 409
    assert r3.json()["error"]["code"] == "idempotency_conflict"


def test_different_key_still_requires_valid_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert _complete(client, t1, "c1", success=True).status_code == 200

    # 任务已结束，使用新 completion_id 必须重新通过租约校验 → invalid_lease
    r = client.post(
        f"/tasks/{t1['task_id']}/complete",
        json={"completion_id": "c2", "lease_token": t1["lease_token"], "success": True},
    )
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "invalid_lease"


def test_idempotent_exhausted_failure_blocks_descendants_once(client):
    dag = submit(client, [{"key": "a", "max_retries": 0}, {"key": "b", "deps": ["a"]}])
    dag_id = dag["id"]
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    payload = {"completion_id": "cf", "lease_token": t["lease_token"],
               "success": False, "error": "fatal"}
    r1 = client.post(f"/tasks/{t['task_id']}/complete", json=payload)
    assert r1.json()["state"] == "failed"
    assert r1.json()["failure_count"] == 1
    assert r1.json()["blocked_descendants"] == 1

    # 任务结束后用相同键重放：仍是首次响应，计数与阻塞都不重复发生
    r2 = client.post(f"/tasks/{t['task_id']}/complete", json=payload)
    assert r2.status_code == 200 and r2.text == r1.text
    full = client.get(f"/dags/{dag_id}").json()
    assert full["states"]["failed"] == 1 and full["states"]["blocked"] == 1
    assert client.get(f"/tasks/{t['task_id']}").json()["failure_count"] == 1


def test_idempotency_record_survives_process_restart(tmp_path, monkeypatch):
    """幂等记录持久化在 SQLite 中：重新打开应用后重放仍返回首次响应。"""
    db_file = tmp_path / "persist.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        submit(c1, [{"key": "a"}])
        t = c1.post("/tasks/claim", json={}).json()["claimed"][0]
        r1 = c1.post(f"/tasks/{t['task_id']}/complete",
                     json={"completion_id": "restart-1",
                           "lease_token": t["lease_token"], "success": True})
        assert r1.status_code == 200

    # 模拟进程重启：重新走 lifespan/init_db，旧库中的 completions 记录仍然有效
    with TestClient(app) as c2:
        r2 = c2.post(f"/tasks/{t['task_id']}/complete",
                     json={"completion_id": "restart-1",
                           "lease_token": t["lease_token"], "success": True})
        assert r2.status_code == 200 and r2.text == r1.text

        r3 = c2.post(f"/tasks/{t['task_id']}/complete",
                     json={"completion_id": "restart-1",
                           "lease_token": t["lease_token"], "success": False})
        assert r3.status_code == 409
        assert r3.json()["error"]["code"] == "idempotency_conflict"


# ------------------------------------------------------------- 重试与阻塞

def test_retry_then_success(client):
    submit(client, [{"key": "a", "max_retries": 2}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-r1",
                          "lease_token": t["lease_token"], "success": False, "error": "boom"})
    assert r.json()["state"] == "pending"
    assert r.json()["retries_remaining"] == 2

    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2
    assert client.post(f"/tasks/{t2['task_id']}/complete",
                       json={"completion_id": "cmp-r2",
                             "lease_token": t2["lease_token"], "success": True}).status_code == 200


def test_exhausted_retries_blocks_descendants(client):
    dag = submit(
        client,
        [
            {"key": "a", "max_retries": 1},
            {"key": "b", "deps": ["a"]},
            {"key": "c", "deps": ["b"]},
        ],
    )
    dag_id = dag["id"]

    for expected_attempt in (1, 2):
        t = client.post("/tasks/claim", json={}).json()["claimed"][0]
        assert t["attempt"] == expected_attempt
        r = client.post(f"/tasks/{t['task_id']}/complete",
                        json={"completion_id": f"cmp-block-{expected_attempt}",
                              "lease_token": t["lease_token"], "success": False, "error": "x"})
    assert r.json()["state"] == "failed"
    assert r.json()["blocked_descendants"] == 2

    # 后继永远领取不到
    assert client.post("/tasks/claim", json={"dag_id": dag_id, "limit": 10}).json()["count"] == 0

    full = client.get(f"/dags/{dag_id}").json()
    b = by_key(full, "b")
    c = by_key(full, "c")
    assert b["state"] == "blocked" and "permanently failed" in b["blocked_reason"]
    assert c["state"] == "blocked" and "permanently failed" in c["blocked_reason"]
    assert full["states"]["failed"] == 1 and full["states"]["blocked"] == 2


def test_lease_expiry_reclaim_does_not_consume_retry_budget(client):
    submit(client, [{"key": "a", "max_retries": 1, "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t1["attempt"] == 1
    time.sleep(0.25)

    # 过期后重领：attempt+1、新令牌，但显式失败计数不变、重试预算保留
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2 and t2["lease_token"] != t1["lease_token"]
    status = client.get(f"/tasks/{t2['task_id']}").json()
    assert status["failure_count"] == 0
    assert status["retries_remaining"] == 1

    # 旧令牌回报失败 -> 409，不得计入失败次数（首次未通过租约校验，不产生幂等记录）
    r = client.post(f"/tasks/{t1['task_id']}/complete",
                    json={"completion_id": "cmp-late",
                          "lease_token": t1["lease_token"], "success": False, "error": "late"})
    assert r.status_code == 409

    # 第一次显式失败：仍可重试（回到 pending），attempts 历史保留过期记录
    r = client.post(f"/tasks/{t2['task_id']}/complete",
                    json={"completion_id": "cmp-e1",
                          "lease_token": t2["lease_token"], "success": False, "error": "e1"})
    assert r.json()["state"] == "pending"
    assert r.json()["failure_count"] == 1
    status = client.get(f"/tasks/{t2['task_id']}").json()
    assert status["state"] == "pending" and status["attempt"] == 2
    assert status["failure_count"] == 1 and status["retries_remaining"] == 0
    assert [a["state"] for a in status["attempts"]] == ["expired", "failed"]

    # 第二次显式失败：预算耗尽 -> failed
    t3 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t3["attempt"] == 3
    r = client.post(f"/tasks/{t3['task_id']}/complete",
                    json={"completion_id": "cmp-e2",
                          "lease_token": t3["lease_token"], "success": False, "error": "e2"})
    assert r.json()["state"] == "failed"
    assert r.json()["failure_count"] == 2
    assert r.json()["retries_remaining"] == 0


def test_retries_remaining_tracks_only_explicit_failures(client):
    submit(client, [{"key": "a", "max_retries": 0, "lease_seconds": 0.2}])
    # 多次过期重领均未显式失败：成功后计数仍为 0
    for expected in (1, 2):
        t = client.post("/tasks/claim", json={}).json()["claimed"][0]
        assert t["attempt"] == expected
        time.sleep(0.25)
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t["attempt"] == 3
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "cmp-ok", "lease_token": t["lease_token"], "success": True})
    assert r.json()["state"] == "succeeded"
    assert r.json()["failure_count"] == 0
    status = client.get(f"/tasks/{t['task_id']}").json()
    assert status["failure_count"] == 0 and status["retries_remaining"] == 0
    assert [a["state"] for a in status["attempts"]] == ["expired", "expired", "succeeded"]


def test_legacy_sqlite_db_gets_failure_count_default(tmp_path, monkeypatch):
    """旧版 tasks 表（无 failure_count 列）在 init_db 时补齐默认值 0。"""
    import sqlite3

    db_file = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE dags (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL, name TEXT NOT NULL,
            payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0, lease_seconds REAL NOT NULL,
            state TEXT NOT NULL, blocked_reason TEXT, lease_token TEXT, lease_expires_at TEXT,
            worker_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            UNIQUE(dag_id, task_key));
        CREATE TABLE dependencies (dag_id TEXT, task_key TEXT, dep_key TEXT,
            PRIMARY KEY (dag_id, task_key, dep_key));
        CREATE TABLE attempts (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT, attempt_no INTEGER,
            lease_token TEXT UNIQUE, worker_id TEXT, leased_at TEXT, expires_at TEXT,
            heartbeat_at TEXT, state TEXT, error TEXT);
        INSERT INTO dags VALUES ('d1', 'old', '2026-01-01T00:00:00Z');
        -- 旧任务 attempt=3（曾因过期被重领），max_retries=1，处于 pending
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t1', 'd1', 'a', 'a', 1, 30, 'pending', 3, '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_file))
    # db_path() 每次调用动态读取环境变量，无需重新导入模块
    from app.db import get_conn, init_db
    init_db()  # 不应报错，且只迁移一次
    init_db()

    conn = sqlite3.connect(db_file)
    row = conn.execute(
        "SELECT attempt, failure_count, max_retries FROM tasks WHERE id = 't1'"
    ).fetchone()
    conn.close()
    assert row == (3, 0, 1)  # 兼容默认值：过期重领不追溯消耗失败预算

    from app import service
    t = service.claim_tasks(None, None, 1)["claimed"][0]
    assert t["attempt"] == 4
    # 第一次显式失败仍有重试额度（旧库迁移后 completions 表可用）
    result = service.complete_task(
        t["task_id"],
        {"completion_id": "cmp-legacy", "lease_token": t["lease_token"], "success": False},
    )
    assert result["state"] == "pending" and result["failure_count"] == 1


# ------------------------------------------------------------- 状态接口

def test_status_lists_attempts_and_lease(client):
    submit(client, [{"key": "a", "lease_seconds": 60}])
    t = client.post("/tasks/claim", json={"worker_id": "w-1"}).json()["claimed"][0]
    client.post(f"/tasks/{t['task_id']}/heartbeat",
                json={"lease_token": t["lease_token"]})

    status = client.get(f"/tasks/{t['task_id']}").json()
    assert status["attempt"] == 1
    assert status["state"] == "running"
    assert status["lease"]["token"] == t["lease_token"]
    assert status["lease"]["expired"] is False
    assert status["lease"]["worker_id"] == "w-1"
    att = status["attempts"][0]
    assert att["attempt"] == 1 and att["state"] == "active"
    assert att["heartbeat_at"] is not None

    # 列表接口支持状态过滤
    r = client.get("/tasks", params={"state": "running"})
    assert r.json()["count"] == 1


# ------------------------------------------------------------- 原子领取
def test_concurrent_claim_is_atomic(client):
    submit(client, [{"key": "a"}])
    results = []
    errors = []

    def worker():
        try:
            r = client.post("/tasks/claim", json={"worker_id": f"w-{threading.get_ident()}"})
            results.extend(r.json()["claimed"])
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    assert len(results) == 1
    assert results[0]["attempt"] == 1


# ------------------------------------------------------------- 事件流

def _all_events(client, **params):
    """逐页拉取全部事件，断言跨页无重复/遗漏。"""
    seen = []
    after = params.get("after", 0)
    while True:
        page = client.get("/events", params={**params, "after": after}).json()
        seqs = [e["sequence"] for e in page["events"]]
        assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))
        seen.extend(page["events"])
        if not page["events"]:
            assert page["next_after"] == after  # 空页沿用 after
            break
        assert page["next_after"] == seqs[-1]
        after = page["next_after"]
    return seen


def test_dag_created_event_nulls_task_fields(client):
    dag = submit(client, [{"key": "a"}], dag_id="dag-evt-1")
    events = client.get("/events", params={"dag_id": "dag-evt-1"}).json()["events"]
    assert len(events) == 1
    e = events[0]
    assert e["sequence"] >= 1
    assert e["type"] == "dag.created"
    assert e["dag_id"] == dag["id"]
    assert e["task_id"] is None
    assert e["attempt"] is None
    assert e["from_state"] is None
    assert e["to_state"] is None
    assert e["occurred_at"]  # 非空时间戳


def test_task_lifecycle_event_sequence(client):
    dag = submit(
        client,
        [
            {"key": "a", "max_retries": 1},
            {"key": "b", "deps": ["a"]},
        ],
    )
    dag_id = dag["id"]

    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    claimed = evts[-1]
    assert claimed["type"] == "task.claimed"
    assert claimed["task_id"] == t["task_id"]
    assert claimed["attempt"] == 1
    assert claimed["from_state"] == "pending"
    assert claimed["to_state"] == "running"

    # 显式失败但仍有额度 -> requeued: running -> pending
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "c-fail", "lease_token": t["lease_token"],
                          "success": False, "error": "boom"})
    assert r.status_code == 200
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    requeued = evts[-1]
    assert requeued["type"] == "task.requeued"
    assert requeued["attempt"] == 1
    assert requeued["from_state"] == "running"
    assert requeued["to_state"] == "pending"

    # 重新领取 -> claimed（pending -> running，attempt=2）
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    claimed2 = evts[-1]
    assert claimed2["type"] == "task.claimed"
    assert claimed2["attempt"] == 2
    assert claimed2["from_state"] == "pending"
    assert claimed2["to_state"] == "running"

    # 再次失败且额度耗尽（max_retries=1，failure_count=2）-> failed，后继 b -> blocked
    client.post(f"/tasks/{t2['task_id']}/complete",
                json={"completion_id": "c-fail-2", "lease_token": t2["lease_token"],
                      "success": False, "error": "fatal"})
    evts = client.get("/events", params={"dag_id": dag_id}).json()["events"]
    failed, blocked = evts[-2], evts[-1]
    assert failed["type"] == "task.failed"
    assert failed["task_id"] == t2["task_id"]
    assert failed["attempt"] == 2
    assert failed["from_state"] == "running" and failed["to_state"] == "failed"
    assert blocked["type"] == "task.blocked"
    assert blocked["task_id"] != t2["task_id"]
    assert blocked["from_state"] == "pending" and blocked["to_state"] == "blocked"

    # sequence 全局严格递增，且 failed 先于 blocked
    seqs = [e["sequence"] for e in evts]
    assert seqs == sorted(set(seqs))
    assert failed["sequence"] < blocked["sequence"]


def test_succeeded_event(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    client.post(f"/tasks/{t['task_id']}/complete",
                json={"completion_id": "c-ok", "lease_token": t["lease_token"], "success": True})
    events = client.get("/events").json()["events"]
    succeeded = events[-1]
    assert succeeded["type"] == "task.succeeded"
    assert succeeded["task_id"] == t["task_id"]
    assert succeeded["attempt"] == 1
    assert succeeded["from_state"] == "running"
    assert succeeded["to_state"] == "succeeded"


def test_claim_after_lease_expiry_event_from_running(client):
    submit(client, [{"key": "a", "lease_seconds": 0.2}])
    t1 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    time.sleep(0.25)
    t2 = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t2["attempt"] == 2
    events = client.get("/events").json()["events"]
    claimed2 = events[-1]
    assert claimed2["type"] == "task.claimed"
    assert claimed2["attempt"] == 2
    assert claimed2["from_state"] == "running"  # 过期重领：任务此前停留在 running
    assert claimed2["to_state"] == "running"


def test_events_pagination_no_gaps_or_duplicates(client):
    # 制造超过单页容量的事件：3 个 DAG，每个 2 个任务走完整流程
    for i in range(3):
        dag = submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id=f"page-{i}")
        for _ in range(2):
            t = client.post("/tasks/claim", json={"dag_id": dag["id"]}).json()["claimed"][0]
            client.post(f"/tasks/{t['task_id']}/complete",
                        json={"completion_id": f"c-{i}-{t['task_key']}",
                              "lease_token": t["lease_token"], "success": True})

    full = client.get("/events", params={"limit": 100}).json()["events"]
    assert len(full) > 3

    # 逐页（limit=1）拉取应与一次性拉取完全一致
    paged = _all_events(client, limit=1)
    assert [e["sequence"] for e in paged] == [e["sequence"] for e in full]

    # 末页之后再翻页：空页沿用 after
    last_seq = full[-1]["sequence"]
    tail = client.get("/events", params={"after": last_seq, "limit": 10}).json()
    assert tail == {"events": [], "next_after": last_seq}


def test_events_filter_by_dag_id_across_global_sequence(client):
    d1 = submit(client, [{"key": "a"}], dag_id="f-1")
    d2 = submit(client, [{"key": "x"}], dag_id="f-2")
    for dag in (d1, d2):
        t = client.post("/tasks/claim", json={"dag_id": dag["id"]}).json()["claimed"][0]
        client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": f"c-{dag['id']}",
                          "lease_token": t["lease_token"], "success": True})

    paged = _all_events(client, dag_id="f-1", limit=1)
    assert {e["dag_id"] for e in paged} == {"f-1"}
    assert [e["type"] for e in paged] == ["dag.created", "task.claimed", "task.succeeded"]

    # 过滤后无新事件：空页沿用传入的全局 after
    tail = client.get("/events", params={"dag_id": "f-1", "after": 10_000}).json()
    assert tail == {"events": [], "next_after": 10_000}


def test_events_default_params(client):
    submit(client, [{"key": "a"}])
    body = client.get("/events").json()
    assert body["next_after"] == body["events"][-1]["sequence"]
    assert body["events"][0]["type"] == "dag.created"


def test_events_rejects_invalid_params(client):
    assert client.get("/events", params={"limit": 0}).status_code == 422
    assert client.get("/events", params={"limit": 101}).status_code == 422
    assert client.get("/events", params={"after": -1}).status_code == 422


def test_idempotent_replay_emits_no_event(client):
    submit(client, [{"key": "a"}])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    payload = {"completion_id": "c1", "lease_token": t["lease_token"], "success": True}
    assert client.post(f"/tasks/{t['task_id']}/complete", json=payload).status_code == 200
    before = client.get("/events", params={"limit": 100}).json()["events"]

    # 幂等重放不产生新事件
    assert client.post(f"/tasks/{t['task_id']}/complete", json=payload).status_code == 200
    after = client.get("/events", params={"limit": 100}).json()["events"]
    assert [e["sequence"] for e in after] == [e["sequence"] for e in before]


def test_failed_submit_rolls_back_event(client):
    submit(client, [{"key": "a"}], dag_id="dup-dag")
    # 重复 DAG id：dag 行插入即冲突，整个事务回滚，不留第二条创建事件
    r = client.post("/dags", json={"id": "dup-dag", "tasks": [{"key": "b"}]})
    assert r.status_code == 409
    events = client.get("/events", params={"dag_id": "dup-dag"}).json()["events"]
    assert len(events) == 1 and events[0]["type"] == "dag.created"

    # 图校验失败（环）发生在开事务之前：完全不留事件
    r = client.post("/dags", json={"tasks": [{"key": "a", "deps": ["a"]}]})
    assert r.status_code == 422
    all_events = client.get("/events", params={"limit": 100}).json()["events"]
    assert len(all_events) == 1


def test_events_persist_across_restart(tmp_path, monkeypatch):
    db_file = tmp_path / "events.db"
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("DEFAULT_LEASE_SECONDS", "30")

    from fastapi.testclient import TestClient
    from app.main import app

    with TestClient(app) as c1:
        submit(c1, [{"key": "a"}], dag_id="restart-dag")

    with TestClient(app) as c2:
        events = c2.get("/events", params={"dag_id": "restart-dag"}).json()["events"]
        assert len(events) == 1
        assert events[0]["type"] == "dag.created"
        assert events[0]["sequence"] == 1  # 重启后 sequence 不重置，不漏不重


def test_event_stream_is_immutable_in_storage(client):
    submit(client, [{"key": "a"}])
    import sqlite3
    from app.db import db_path

    conn = sqlite3.connect(db_path())
    with pytest.raises(sqlite3.Error):
        conn.execute("UPDATE events SET event_type = 'tampered' WHERE sequence = 1")
    conn.rollback()
    with pytest.raises(sqlite3.Error):
        conn.execute("DELETE FROM events")
    conn.rollback()
    conn.close()

    events = client.get("/events").json()["events"]
    assert events[0]["type"] == "dag.created"


# ------------------------------------------------------------- 只读分析

def test_analysis_computes_schedule_critical_path_and_levels(client):
    submit(
        client,
        [
            {"key": "a", "duration_seconds": 3},
            {"key": "b", "deps": ["a"], "duration_seconds": 2},
            {"key": "c", "deps": ["a"], "duration_seconds": 5},
            {"key": "d", "deps": ["b", "c"]},  # 未声明时长 -> 默认 1
        ],
        dag_id="ana-1",
    )
    r = client.get("/dags/ana-1/analysis")
    assert r.status_code == 200
    body = r.json()
    assert body["dag_id"] == "ana-1"
    assert body["task_count"] == 4

    tasks = {t["key"]: t for t in body["tasks"]}
    # a: 0-3；b: 3-5；c: 3-8；d: 8-9
    assert (tasks["a"]["earliest_start"], tasks["a"]["earliest_finish"]) == (0, 3)
    assert (tasks["b"]["earliest_start"], tasks["b"]["earliest_finish"]) == (3, 5)
    assert (tasks["c"]["earliest_start"], tasks["c"]["earliest_finish"]) == (3, 8)
    assert (tasks["d"]["earliest_start"], tasks["d"]["earliest_finish"]) == (8, 9)
    assert tasks["d"]["duration_seconds"] == 1  # 默认时长

    assert body["total_duration_seconds"] == 9
    assert body["critical_path"] == ["a", "c", "d"]
    assert body["critical_tasks"] == ["a", "c", "d"]
    assert tasks["a"]["critical"] is True and tasks["a"]["slack"] == 0
    assert tasks["b"]["critical"] is False and tasks["b"]["slack"] == 3
    assert tasks["b"]["latest_start"] == 6 and tasks["b"]["latest_finish"] == 8

    # 并行层级：同层互不依赖
    assert body["levels"] == [["a"], ["b", "c"], ["d"]]
    assert [tasks[k]["level"] for k in "abcd"] == [0, 1, 1, 2]


def test_analysis_default_duration_is_one(client):
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="ana-def")
    body = client.get("/dags/ana-def/analysis").json()
    assert body["total_duration_seconds"] == 2
    assert body["critical_path"] == ["a", "b"]
    assert body["critical_tasks"] == ["a", "b"]
    assert body["levels"] == [["a"], ["b"]]


def test_analysis_fractional_durations(client):
    submit(
        client,
        [
            {"key": "a", "duration_seconds": 0.5},
            {"key": "b", "deps": ["a"], "duration_seconds": 0.25},
        ],
        dag_id="ana-frac",
    )
    body = client.get("/dags/ana-frac/analysis").json()
    assert body["total_duration_seconds"] == 0.75
    assert body["critical_path"] == ["a", "b"]


def test_analysis_tied_critical_paths_are_deterministic(client):
    # 两条等长关键路径 a->c 与 b->c：关键任务集合包含全部，路径确定唯一
    submit(
        client,
        [
            {"key": "a", "duration_seconds": 2},
            {"key": "b", "duration_seconds": 2},
            {"key": "c", "deps": ["a", "b"], "duration_seconds": 1},
        ],
        dag_id="ana-tie",
    )
    body = client.get("/dags/ana-tie/analysis").json()
    assert body["total_duration_seconds"] == 3
    assert body["critical_tasks"] == ["a", "b", "c"]
    assert body["critical_path"] == ["a", "c"]  # 平局取较小 key
    assert client.get("/dags/ana-tie/analysis").json() == body  # 可重复且一致


def test_analysis_unknown_dag_returns_404(client):
    r = client.get("/dags/nope/analysis")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "dag_not_found"


def test_submit_rejects_non_positive_duration(client):
    for bad in (0, -2, -0.5):
        r = client.post("/dags", json={"tasks": [{"key": "a", "duration_seconds": bad}]})
        assert r.status_code == 422


def test_analysis_is_read_only(client):
    submit(
        client,
        [{"key": "a", "duration_seconds": 2}, {"key": "b", "deps": ["a"]}],
        dag_id="ana-ro",
    )
    before_events = client.get("/events", params={"dag_id": "ana-ro"}).json()
    before_dag = client.get("/dags/ana-ro").json()

    r1 = client.get("/dags/ana-ro/analysis")
    r2 = client.get("/dags/ana-ro/analysis")
    assert r1.status_code == 200 and r1.json() == r2.json()

    # 分析不改变任务状态，也不产生事件
    assert client.get("/events", params={"dag_id": "ana-ro"}).json() == before_events
    assert client.get("/dags/ana-ro").json() == before_dag


def test_analysis_rejects_corrupt_duration_in_storage(client):
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="ana-bad")
    import sqlite3
    from app.db import db_path

    conn = sqlite3.connect(db_path())
    conn.execute(
        "UPDATE tasks SET duration_seconds = 0 WHERE dag_id = 'ana-bad' AND task_key = 'a'"
    )
    conn.commit()
    conn.close()

    r = client.get("/dags/ana-bad/analysis")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_duration"
    assert r.json()["error"]["details"]["keys"] == ["a"]


def test_analysis_rejects_injected_cycle(client):
    submit(client, [{"key": "a"}, {"key": "b", "deps": ["a"]}], dag_id="ana-cyc")
    import sqlite3
    from app.db import db_path

    # 绕过提交校验直接写入一条成环的边
    conn = sqlite3.connect(db_path())
    conn.execute(
        "INSERT INTO dependencies (dag_id, task_key, dep_key) VALUES ('ana-cyc', 'a', 'b')"
    )
    conn.commit()
    conn.close()

    r = client.get("/dags/ana-cyc/analysis")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "cycle_detected"


def test_analysis_rejects_injected_missing_dependency(client):
    submit(client, [{"key": "a"}], dag_id="ana-miss")
    import sqlite3
    from app.db import db_path

    conn = sqlite3.connect(db_path())
    conn.execute(
        "INSERT INTO dependencies (dag_id, task_key, dep_key) VALUES ('ana-miss', 'a', 'ghost')"
    )
    conn.commit()
    conn.close()

    r = client.get("/dags/ana-miss/analysis")
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "missing_dependency"
    assert "ghost" in r.json()["error"]["details"]["missing"]


def test_duration_seconds_exposed_in_task_views(client):
    dag = submit(client, [{"key": "a", "duration_seconds": 7}], dag_id="ana-view")
    assert by_key(dag, "a")["duration_seconds"] == 7

    overview = client.get("/dags/ana-view").json()
    assert by_key(overview, "a")["duration_seconds"] == 7

    task_id = overview["tasks"][0]["id"]
    detail = client.get(f"/tasks/{task_id}").json()
    assert detail["duration_seconds"] == 7


def test_legacy_sqlite_db_gets_duration_default(tmp_path, monkeypatch):
    """旧版 tasks 表（无 duration_seconds 列）迁移后默认 1，分析可直接使用。"""
    import sqlite3

    db_file = tmp_path / "legacy-analysis.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE dags (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL, name TEXT NOT NULL,
            payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0, lease_seconds REAL NOT NULL,
            state TEXT NOT NULL, blocked_reason TEXT, lease_token TEXT, lease_expires_at TEXT,
            worker_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            UNIQUE(dag_id, task_key));
        CREATE TABLE dependencies (dag_id TEXT, task_key TEXT, dep_key TEXT,
            PRIMARY KEY (dag_id, task_key, dep_key));
        INSERT INTO dags VALUES ('d1', 'old', '2026-01-01T00:00:00Z');
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t1', 'd1', 'a', 'a', 0, 30, 'pending', 0, '2026-01-01T00:00:00Z');
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t2', 'd1', 'b', 'b', 0, 30, 'pending', 0, '2026-01-01T00:00:00Z');
        INSERT INTO dependencies VALUES ('d1', 'b', 'a');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_file))
    from app.db import init_db
    from app import service

    init_db()
    init_db()  # 迁移幂等

    analysis = service.analyze_dag("d1")
    assert analysis["total_duration_seconds"] == 1 + 1  # 历史任务默认时长 1
    assert analysis["critical_path"] == ["a", "b"]
    assert analysis["levels"] == [["a"], ["b"]]


# ------------------------------------------------------------- 跨 DAG 并发组限流

def test_group_and_limit_must_be_provided_together(client):
    r = client.post("/dags", json={"tasks": [{"key": "a", "concurrency_group": "gpu"}]})
    assert r.status_code == 422
    r = client.post("/dags", json={"tasks": [{"key": "a", "concurrency_limit": 2}]})
    assert r.status_code == 422


def test_group_limit_must_be_positive_integer(client):
    for bad in (0, -1, 1.5):
        r = client.post(
            "/dags",
            json={"tasks": [{"key": "a", "concurrency_group": "gpu",
                             "concurrency_limit": bad}]},
        )
        assert r.status_code == 422


def test_conflicting_limit_within_single_submit_rolls_back(client):
    r = client.post(
        "/dags",
        json={
            "id": "cg-self-conflict",
            "tasks": [
                {"key": "a", "concurrency_group": "gpu", "concurrency_limit": 2},
                {"key": "b", "concurrency_group": "gpu", "concurrency_limit": 3},
            ],
        },
    )
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "concurrency_limit_conflict"
    # 校验发生在开事务之前：不留 DAG，也不留事件
    assert client.get("/dags/cg-self-conflict").status_code == 404
    assert client.get("/events", params={"dag_id": "cg-self-conflict"}).json()["events"] == []


def test_conflicting_limit_with_persisted_group_rolls_back(client):
    submit(client, [{"key": "a", "concurrency_group": "gpu", "concurrency_limit": 1}],
           dag_id="cg-first")

    r = client.post(
        "/dags",
        json={"id": "cg-second",
              "tasks": [{"key": "x", "concurrency_group": "gpu", "concurrency_limit": 5}]},
    )
    assert r.status_code == 409
    body = r.json()["error"]
    assert body["code"] == "concurrency_limit_conflict"
    assert body["details"]["group"] == "gpu"
    assert body["details"]["existing_limit"] == 1
    assert body["details"]["requested_limit"] == 5

    # 冲突提交整体回滚：DAG 不存在、无事件，已持久化的组上限不被污染
    assert client.get("/dags/cg-second").status_code == 404
    assert client.get("/events", params={"dag_id": "cg-second"}).json()["events"] == []
    dag = submit(client, [{"key": "x", "concurrency_group": "gpu", "concurrency_limit": 1}],
                 dag_id="cg-second")
    assert dag["task_count"] == 1


def test_same_limit_group_shared_across_dags(client):
    submit(client, [{"key": "a", "concurrency_group": "io", "concurrency_limit": 2}],
           dag_id="cg-dag-1")
    submit(client, [{"key": "b", "concurrency_group": "io", "concurrency_limit": 2}],
           dag_id="cg-dag-2")
    submit(client, [{"key": "c", "concurrency_group": "io", "concurrency_limit": 2}],
           dag_id="cg-dag-3")

    # 组上限跨 DAG 共享：三个 DAG 各有一个可领任务，全局最多同时运行 2 个
    claimed = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert len(claimed) == 2
    assert {t["concurrency_group"] for t in claimed} == {"io"}
    assert client.post("/tasks/claim", json={"limit": 10}).json()["count"] == 0

    # 完成其中一个后名额释放，第三个 DAG 的任务可领
    t = claimed[0]
    assert client.post(f"/tasks/{t['task_id']}/complete",
                       json={"completion_id": "cg-io-1", "lease_token": t["lease_token"],
                             "success": True}).status_code == 200
    claimed2 = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert len(claimed2) == 1
    # 新领到的是第三个 DAG 的任务（前两个 DAG 的任务一个已完成、一个仍在运行）
    assert claimed2[0]["dag_id"] not in {t["dag_id"] for t in claimed}


def test_full_group_is_skipped_and_limit_filled_with_others(client):
    # 组 gpu 上限 2：3 个同组任务一次最多领 2 个；无组任务不受限，尽量填满 limit
    submit(client, [
        {"key": "g1", "concurrency_group": "gpu", "concurrency_limit": 2},
        {"key": "g2", "concurrency_group": "gpu", "concurrency_limit": 2},
        {"key": "g3", "concurrency_group": "gpu", "concurrency_limit": 2},
        {"key": "p1"},
        {"key": "p2"},
    ])
    claimed = client.post("/tasks/claim", json={"limit": 4}).json()["claimed"]
    keys = [t["task_key"] for t in claimed]
    assert len(keys) == 4  # 2 个组任务 + 2 个无组任务，正好填满 limit
    assert sum(1 for k in keys if k.startswith("g")) == 2
    assert sorted(k for k in keys if k.startswith("p")) == ["p1", "p2"]

    # 组满且无其他可领任务：只剩 g3，被跳过
    assert client.post("/tasks/claim", json={"limit": 10}).json()["count"] == 0


def test_batch_claim_counts_new_claims_against_group_limit(client):
    # 同一批次内新领取的任务立即占位：上限 2、4 个候选，一次批量领取不会突破
    submit(client, [
        {"key": k, "concurrency_group": "batch", "concurrency_limit": 2}
        for k in ("a", "b", "c", "d")
    ])
    claimed = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert len(claimed) == 2
    assert all(t["concurrency_group"] == "batch" for t in claimed)
    assert all(t["concurrency_limit"] == 2 for t in claimed)
    assert client.post("/tasks/claim", json={"limit": 10}).json()["count"] == 0


def test_group_slot_released_on_completion(client):
    submit(client, [
        {"key": "a", "concurrency_group": "rel", "concurrency_limit": 1},
        {"key": "b", "concurrency_group": "rel", "concurrency_limit": 1},
    ])
    claimed = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert len(claimed) == 1  # 上限 1：同批也只领一个
    t = claimed[0]
    client.post(f"/tasks/{t['task_id']}/complete",
                json={"completion_id": "rel-1", "lease_token": t["lease_token"],
                      "success": True})
    # 完成后名额释放，同组另一个任务可领
    claimed2 = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert len(claimed2) == 1
    assert claimed2[0]["task_id"] != t["task_id"]


def test_group_slot_released_on_requeue_and_final_failure(client):
    submit(client, [
        {"key": "a", "concurrency_group": "relf", "concurrency_limit": 1, "max_retries": 1},
        {"key": "b", "concurrency_group": "relf", "concurrency_limit": 1},
    ])
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert client.post("/tasks/claim", json={"limit": 10}).json()["count"] == 0

    # 显式失败但仍有重试额度：回到 pending，名额随之释放（否则一个都领不到）
    r = client.post(f"/tasks/{t['task_id']}/complete",
                    json={"completion_id": "relf-1", "lease_token": t["lease_token"],
                          "success": False, "error": "boom"})
    assert r.json()["state"] == "pending"
    claimed = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert len(claimed) == 1

    # 最终失败（重试耗尽）同样释放名额
    t2 = claimed[0]
    client.post(f"/tasks/{t2['task_id']}/complete",
                json={"completion_id": "relf-2", "lease_token": t2["lease_token"],
                      "success": False, "error": "fatal"})
    claimed2 = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert len(claimed2) == 1


def test_group_slot_released_on_lease_expiry(client):
    submit(client, [
        {"key": "a", "concurrency_group": "rele", "concurrency_limit": 1,
         "lease_seconds": 0.2},
    ], dag_id="cg-exp-1")
    t = client.post("/tasks/claim", json={}).json()["claimed"][0]
    assert t["task_key"] == "a"
    # 同组新 DAG 的任务在租约未过期时无法领取（组占用跨 DAG 统计）
    submit(client, [{"key": "b", "concurrency_group": "rele", "concurrency_limit": 1}],
           dag_id="cg-exp-2")
    assert client.post("/tasks/claim", json={"limit": 10}).json()["count"] == 0
    time.sleep(0.25)  # 租约过期：名额自动释放，无需任何回报
    assert client.post("/tasks/claim", json={"limit": 10}).json()["count"] == 1


def test_ungrouped_tasks_unaffected_by_group_limits(client):
    # 无组任务不受任何组上限约束，行为与未声明组时一致
    submit(client, [
        {"key": "g", "concurrency_group": "iso", "concurrency_limit": 1},
        {"key": "p1"},
        {"key": "p2"},
        {"key": "p3"},
    ])
    claimed = client.post("/tasks/claim", json={"limit": 10}).json()["claimed"]
    assert len(claimed) == 4  # 组任务 1 个（上限）+ 全部 3 个无组任务
    assert sum(1 for t in claimed if t["concurrency_group"] is None) == 3


def test_concurrent_claims_never_exceed_group_limit(client):
    submit(client, [
        {"key": k, "concurrency_group": "race", "concurrency_limit": 2}
        for k in ("a", "b", "c", "d")
    ])
    results = []
    errors = []

    def worker():
        try:
            r = client.post("/tasks/claim",
                            json={"worker_id": f"w-{threading.get_ident()}", "limit": 4})
            results.extend(r.json()["claimed"])
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert not errors
    assert len(results) == 2  # 多线程批量领取也不会突破全局组上限
    assert len({t["task_id"] for t in results}) == 2


def test_concurrency_group_exposed_in_task_views(client):
    dag = submit(client, [{"key": "a", "concurrency_group": "gpu", "concurrency_limit": 3}],
                 dag_id="cg-view")
    assert by_key(dag, "a")["concurrency_group"] == "gpu"
    assert by_key(dag, "a")["concurrency_limit"] == 3

    overview = client.get("/dags/cg-view").json()
    assert by_key(overview, "a")["concurrency_group"] == "gpu"
    assert by_key(overview, "a")["concurrency_limit"] == 3

    task_id = overview["tasks"][0]["id"]
    detail = client.get(f"/tasks/{task_id}").json()
    assert detail["concurrency_group"] == "gpu"
    assert detail["concurrency_limit"] == 3

    listed = client.get("/tasks", params={"dag_id": "cg-view"}).json()["tasks"]
    assert listed[0]["concurrency_group"] == "gpu"
    assert listed[0]["concurrency_limit"] == 3

    # 领取响应同样携带组信息
    claimed = client.post("/tasks/claim", json={"dag_id": "cg-view"}).json()["claimed"][0]
    assert claimed["concurrency_group"] == "gpu"
    assert claimed["concurrency_limit"] == 3

    # 无组任务两个字段均为 null
    dag2 = submit(client, [{"key": "plain"}], dag_id="cg-view-2")
    assert by_key(dag2, "plain")["concurrency_group"] is None
    assert by_key(dag2, "plain")["concurrency_limit"] is None


def test_legacy_sqlite_db_gets_concurrency_columns(tmp_path, monkeypatch):
    """旧版 tasks 表（无并发组列）迁移后两列为 NULL，任务按无组处理。"""
    import sqlite3

    db_file = tmp_path / "legacy-cg.db"
    conn = sqlite3.connect(db_file)
    conn.executescript(
        """
        CREATE TABLE dags (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_key TEXT NOT NULL, name TEXT NOT NULL,
            payload TEXT, max_retries INTEGER NOT NULL DEFAULT 0, lease_seconds REAL NOT NULL,
            state TEXT NOT NULL, blocked_reason TEXT, lease_token TEXT, lease_expires_at TEXT,
            worker_id TEXT, attempt INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            UNIQUE(dag_id, task_key));
        CREATE TABLE dependencies (dag_id TEXT, task_key TEXT, dep_key,
            PRIMARY KEY (dag_id, task_key, dep_key));
        INSERT INTO dags VALUES ('d1', 'old', '2026-01-01T00:00:00Z');
        INSERT INTO tasks (id, dag_id, task_key, name, max_retries, lease_seconds, state,
                           attempt, created_at)
        VALUES ('t1', 'd1', 'a', 'a', 0, 30, 'pending', 0, '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", str(db_file))
    from app.db import init_db
    from app import service

    init_db()
    init_db()  # 迁移幂等

    task = service.get_task("t1")
    assert task["concurrency_group"] is None
    assert task["concurrency_limit"] is None
    # 无组任务照常可领
    assert service.claim_tasks(None, None, 1)["count"] == 1
