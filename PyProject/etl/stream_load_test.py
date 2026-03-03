#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最简 StarRocks Stream Load 压测脚本（单集群；无重试；按天生成 event_date；进度日志）

用途：
- 读取 combined-1g.csv（首行列名，且不含 event_date）
- 生成连续 N 天（默认 100）作为 event_date；每天写入一次同一文件
- 单次运行仅一种并发（--concurrency）
- 每次成功写入打印一行进度日志；末尾再打印汇总

示例：
python3 starrocks_streamload_simple.py \
  --fe http://127.0.0.1:8040 \
  --db test --table binance_futures_trades_raw_ssd \
  --user root --password 'Unicode@0sr' \
  --file combined-1g.csv \
  --start-date 2025-02-01 --days 100 \
  --concurrency 4
"""


import argparse
import concurrent.futures as futures
import datetime as dt
import json
import os
import threading
import time
import uuid
from typing import List, Dict, Any, Tuple

import requests


class LoadSession(requests.Session):
    def rebuild_auth(self, prepared_request, response):
        """保留 Authorization 头以应对 307/308 重定向到 Leader。"""
        return

def stream_load_once(
    fe_base: str,
    db: str,
    table: str,
    user: str,
    password: str,
    data_bytes: bytes,  # 改为接收字节数据
    label: str,
    event_date: str,
    csv_columns: List[str],
    sep: str = ",",
    skip_header: int = 1,
    timeout: int = 1800,
) -> Tuple[bool, float, Dict[str, Any]]:
    url = f"{fe_base.rstrip('/')}/api/{db}/{table}/_stream_load"
    columns_directive = ",".join(csv_columns + [f'event_date = "{event_date}"'])
    headers = {
        "label": label,
        "format": "csv",
        "column_separator": sep,
        "skip_header": str(skip_header),
        "columns": columns_directive,
        "Expect": "100-continue",
        "Content-Type": "application/octet-stream",
    }
    session = LoadSession()
    session.auth = (user, password)

    start = time.perf_counter()
    # 直接使用内存中的字节数据
    resp = session.put(url, headers=headers, data=data_bytes, timeout=timeout)
    dur = time.perf_counter() - start

    try:
        payload = resp.json()
    except Exception:
        payload = {"text": resp.text}

    ok = (resp.status_code == 200) and (
        (isinstance(payload, dict) and payload.get("Status") == "Success")
        or (isinstance(payload, dict) and payload.get("status") == "Success")
    )
    return ok, dur, payload



def daterange(start_date: dt.date, days: int) -> List[str]:
    return [(start_date + dt.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]


def percentile(arr: List[float], p: float) -> float:
    if not arr:
        return 0.0
    s = sorted(arr)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] * (c - k) + s[c] * (k - f)


def run_once(
        fe: str,
        db: str,
        table: str,
        user: str,
        password: str,
        file_path: str,
        dates: List[str],
        concurrency: int,
        csv_columns: List[str],
        sep: str,
        skip_header: int,
        label_prefix: str,
        timeout: int,
):
    # 预加载文件到内存
    print(f"正在加载文件到内存: {file_path}")
    load_start = time.perf_counter()
    with open(file_path, "rb") as f:
        file_data = f.read()
    file_size_mb = len(file_data) / (1024 ** 2)
    load_time = time.perf_counter() - load_start
    print(f"文件已加载: {file_size_mb:.2f} MB，耗时 {load_time:.2f}s")

    lock = threading.Lock()
    stats = {"success": 0, "fail": 0, "durations": [], "done": 0}
    total = len(dates)

    def safe_get(d: Dict[str, Any], *keys, default=None):
        for k in keys:
            if isinstance(d, dict) and k in d:
                return d[k]
        return default

    def worker(day: str):
        nonlocal stats
        label = f"{label_prefix}_{concurrency}_{day}_{uuid.uuid4().hex[:8]}"
        # 使用内存中的数据
        ok, dur, payload = stream_load_once(
            fe, db, table, user, password, file_data,  # 传递字节数据
            label=label,
            event_date=day,
            csv_columns=csv_columns,
            sep=sep,
            skip_header=skip_header,
            timeout=timeout,
        )
        number_rows = safe_get(payload, "NumberLoadedRows", "NumberTotalRows", default="?")
        load_bytes = safe_get(payload, "LoadBytes", default=None)
        load_gb = (float(load_bytes) / (1000 ** 3)) if isinstance(load_bytes, (int, float)) else None
        with lock:
            stats["durations"].append(dur)
            stats["done"] += 1
            if ok:
                stats["success"] += 1
                gb_str = f" {load_gb:.3f}GB" if load_gb is not None else ""
                print(f"[OK] {stats['done']}/{total} day={day} label={label} dur={dur:.2f}s rows={number_rows}{gb_str}")
            else:
                stats["fail"] += 1
                msg = json.dumps(payload)
                print(f"[FAIL] {stats['done']}/{total} day={day} label={label} dur={dur:.2f}s payload={msg}")

    wall_start = time.perf_counter()
    with futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
        list(ex.map(worker, dates))
    wall = time.perf_counter() - wall_start

    succ = stats["success"]
    fail = stats["fail"]
    file_gb = len(file_data) / (1000 ** 3)  # 使用实际字节大小
    total_gb = succ * file_gb
    avg = sum(stats["durations"]) / len(stats["durations"]) if stats["durations"] else 0.0
    p95 = percentile(stats["durations"], 95)
    tput_gbps = total_gb / wall if wall > 0 else 0.0

    print(
        f"""
            ==== 并发 {concurrency} 结果 ====
            成功/失败: {succ}/{fail}
            框钟总耗时: {wall:.2f}s (~{wall / 60.0:.2f}min)
            累计写入: ~{total_gb:.2f} GB (文件约 {file_gb:.3f} GB × 成功 {succ} 天)
            平均单次耗时: {avg:.2f}s | P95: {p95:.2f}s
            总体吞吐: {tput_gbps:.3f} GB/s ({tput_gbps * 60:.1f} GB/min)""")

def main():
    ap = argparse.ArgumentParser(description="最简 StarRocks Stream Load 压测（按天生成 event_date）")
    ap.add_argument("--fe", required=True, help="FE 基地址，如 http://fe:8030")
    ap.add_argument("--db", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--file", required=True, help="combined-1g.csv 路径（首行列名，不含 event_date）")
    ap.add_argument("--start-date", required=True, help="起始日期 YYYY-MM-DD，如 2025-01-01")
    ap.add_argument("--days", type=int, default=100, help="天数，默认 100")
    ap.add_argument("--concurrency", type=int, default=1, help="并发数量")
    ap.add_argument(
        "--csv-columns",
        default="exchange,symbol,exchange_ts_ns,local_ts_ns,id,side,price,quantity",
        help="CSV 中的列名顺序（不含 event_date），逗号分隔；需与 CSV 实际列顺序一致",
    )
    ap.add_argument("--sep", default=",", help="CSV 分隔符，默认 ,")
    ap.add_argument("--skip-header", type=int, default=1, help="跳过首行列名 1/0")
    ap.add_argument("--timeout", type=int, default=1800)
    ap.add_argument("--label-prefix", default="bench")
    args = ap.parse_args()

    csv_cols = [c.strip() for c in args.csv_columns.split(",") if c.strip()]
    dates = daterange(dt.datetime.strptime(args.start_date, "%Y-%m-%d").date(), args.days)

    print(f"目标表: {args.db}.{args.table}")
    print(f"FE: {args.fe}  文件: {args.file}  天数: {len(dates)} ({dates[0]} ~ {dates[-1]})")
    print(f"并发: {args.concurrency}  CSV列: {csv_cols}")

    run_once(
        fe=args.fe,
        db=args.db,
        table=args.table,
        user=args.user,
        password=args.password,
        file_path=args.file,
        dates=dates,
        concurrency=args.concurrency,
        csv_columns=csv_cols,
        sep=args.sep,
        skip_header=args.skip_header,
        label_prefix=args.label_prefix,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    main()
