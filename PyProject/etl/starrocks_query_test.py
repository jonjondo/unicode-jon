#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
StarRocks 查询压测脚本（复杂版，尽量全量数据 + 多样 JOIN）
- 通过 MySQL 协议连接 FE（9030）
- 内置 12 条更复杂的 SQL（多 CTE、自连接、聚合后回连、ROLLUP、Top-N、分位数等）
- 尽量不加"近 N 天"过滤，默认跑全量（请确保集群资源允许）
- 支持预热/重复，输出逐条耗时与汇总 avg/p95

示例：
python starrocks_query_test.py \
  --host 192.168.6.12 --port 9030 \
  --user root --password 'Unicode@0sr' \
  --db test \
  --table binance_futures_trades_raw_ssd \
  --repeat 3 \
  --output query_bench.csv
"""

import argparse
import csv
import sys
import time
from typing import List, Dict, Any

try:
    import pymysql
except ImportError:
    print("需要 pymysql：pip install pymysql", file=sys.stderr)
    sys.exit(1)


def get_sqls(table_name: str) -> List[Dict[str, Any]]:
    """动态生成 SQL 列表，支持自定义表名"""
    return [
        {
            "id": 1,
            "name": "全量：每symbol VWAP 与总量（聚合后回连）",
            "sql": f"""
            SELECT s.symbol, s.pv/s.q AS vwap, s.q AS qty
            FROM (
              SELECT symbol,
                     SUM(price*quantity) AS pv,
                     SUM(quantity) AS q
              FROM {table_name}
              GROUP BY symbol
            ) s
            WHERE s.q > 0
            ORDER BY qty DESC
            LIMIT 100
            """,
        },
        {
            "id": 2,
            "name": "全量：日×symbol 聚合后与总表 JOIN 求均价偏离",
            "sql": f"""
                SELECT d.symbol, d.event_date, d.avg_price, s.vwap_all,
                       (d.avg_price - s.vwap_all) AS dev
                FROM (
                  SELECT event_date, symbol,
                         AVG(price) AS avg_price,
                         SUM(quantity) AS qty
                  FROM {table_name}
                  GROUP BY event_date, symbol
                ) d
                JOIN (
                  SELECT symbol, SUM(price*quantity)/SUM(quantity) AS vwap_all
                  FROM {table_name}
                  GROUP BY symbol
                ) s ON d.symbol = s.symbol
                ORDER BY ABS(dev) DESC
                LIMIT 100
                """,
        },
        {
            "id": 3,
            "name": "全量：日环比（DoD）自连接",
            "sql": f"""
                SELECT a.symbol, a.event_date, a.qty, a.avg_price,
                       a.qty - b.qty AS qty_dod_abs,
                       CASE WHEN b.qty>0 THEN (a.qty - b.qty)/b.qty END AS qty_dod_pct,
                       a.avg_price - b.avg_price AS price_dod_abs
                FROM (
                  SELECT event_date, symbol,
                         SUM(quantity) AS qty,
                         AVG(price) AS avg_price
                  FROM {table_name}
                  GROUP BY event_date, symbol
                ) a
                LEFT JOIN (
                  SELECT event_date, symbol,
                         SUM(quantity) AS qty,
                         AVG(price) AS avg_price
                  FROM {table_name}
                  GROUP BY event_date, symbol
                ) b ON a.symbol = b.symbol AND b.event_date = DATE_SUB(a.event_date, INTERVAL 1 DAY)
                ORDER BY a.symbol, a.event_date
                LIMIT 100
                """,
        },
        {
            "id": 4,
            "name": "全量：方向不对称（聚合后 JOIN）",
            "sql": f"""
            SELECT b.symbol, b.event_date, b.buy_qty, b.sell_qty,
                   CASE WHEN b.sell_qty>0 THEN b.buy_qty/b.sell_qty END AS qty_ratio,
                   (b.buy_avg - b.sell_avg) AS avg_price_spread,
                   s.total_qty
            FROM (
              SELECT event_date, symbol,
                     SUM(CASE WHEN side='buy' THEN quantity ELSE 0 END) AS buy_qty,
                     SUM(CASE WHEN side='sell' THEN quantity ELSE 0 END) AS sell_qty,
                     AVG(CASE WHEN side='buy' THEN price END) AS buy_avg,
                     AVG(CASE WHEN side='sell' THEN price END) AS sell_avg
              FROM {table_name}
              GROUP BY event_date, symbol
            ) b
            JOIN (
              SELECT symbol, SUM(quantity) AS total_qty
              FROM {table_name}
              GROUP BY symbol
            ) s ON b.symbol = s.symbol
            ORDER BY b.event_date DESC, qty_ratio DESC
            LIMIT 100
            """,
        },
        {
            "id": 5,
            "name": "全量：Top-N（每 symbol 的按日成交量 Top5）",
            "sql": f"""
            SELECT symbol, event_date, qty
            FROM (
              SELECT symbol, event_date, qty,
                     ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY qty DESC) AS rn
              FROM (
                SELECT event_date, symbol, SUM(quantity) AS qty
                FROM {table_name}
                GROUP BY event_date, symbol
              ) d
            ) r
            WHERE rn <= 5
            ORDER BY symbol, qty DESC
            LIMIT 100
            """,
        },
        {
            "id": 6,
            "name": "全量：ROLLUP（日×symbol、仅日、全局）",
            "sql": f"""
            SELECT event_date, symbol, SUM(quantity) AS qty
            FROM {table_name}
            GROUP BY ROLLUP(event_date, symbol)
            ORDER BY event_date DESC NULLS LAST, qty DESC
            LIMIT 100
            """,
                    },
                    {
                        "id": 7,
                        "name": "全量：价格分位（P99）与重尾排序",
                        "sql": f"""
            SELECT symbol,
                   PERCENTILE_APPROX(quantity, 0.99) AS q99,
                   SUM(quantity) AS qty_sum
            FROM {table_name}
            GROUP BY symbol
            ORDER BY q99 DESC
            LIMIT 100
            """,
        },
        {
            "id": 8,
            "name": "全量：窗口移动平均（MA14）与异常检测",
            "sql": f"""
            SELECT *
            FROM (
              SELECT symbol, event_date, avg_p,
                     AVG(avg_p) OVER (PARTITION BY symbol ORDER BY event_date 
                                      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS ma14,
                     STDDEV_SAMP(avg_p) OVER (PARTITION BY symbol ORDER BY event_date 
                                              ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS sd14
              FROM (
                SELECT event_date, symbol, AVG(price) AS avg_p
                FROM {table_name}
                GROUP BY event_date, symbol
              ) d
            ) z
            WHERE sd14 IS NOT NULL AND ABS(avg_p - ma14) > 2*sd14
            ORDER BY symbol, event_date
            LIMIT 100
            """,
        },
        {
            "id": 9,
            "name": "简单汇总",
            "sql": f"""
            SELECT COUNT(*) AS cnt
              FROM {table_name}
            LIMIT 200
            """,
        },
        {
            "id": 10,
            "name": "各币种数据量",
            "sql": f"""
            SELECT symbol, COUNT(*) AS cnt
            FROM {table_name}
            GROUP BY symbol
            ORDER BY cnt
            LIMIT 200
            """,
                    },
            {
                        "id": 11,
                        "name": "全量：近似去重交易ID（每 symbol）",
                        "sql": f"""
            SELECT symbol,
                   APPROX_COUNT_DISTINCT(id) AS approx_distinct_trades
            FROM {table_name}
            GROUP BY symbol
            ORDER BY symbol
            LIMIT 200
            """,
        },
        {
            "id": 12,
            "name": "各日期各币种数据量",
            "sql": f"""
            SELECT symbol,event_date, COUNT(*) AS cnt
            FROM {table_name}
            GROUP BY symbol,event_date
            ORDER BY cnt
            LIMIT 200
            """,
        },
    ]


def percentile(arr: List[float], p: float) -> float:
    """计算百分位数"""
    if not arr:
        return 0.0
    s = sorted(arr)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] * (c - k) + s[c] * (k - f)


def run_warmup(conn, sqls: List[Dict[str, Any]], warmup: int, only_ids: List[int]):
    """预热查询"""
    if warmup <= 0:
        return

    print(f"开始预热 {warmup} 次...\n")
    cur = conn.cursor()
    selected = [q for q in sqls if (not only_ids or q["id"] in only_ids)]

    for q in selected:
        for w in range(1, warmup + 1):
            try:
                start = time.perf_counter()
                cur.execute(q["sql"])
                _ = cur.fetchall()  # 确保数据传输完成
                elapsed = time.perf_counter() - start
                print(f"[WARMUP] id={q['id']:>2} round={w}/{warmup} name={q['name']} time={elapsed:.3f}s")
            except Exception as e:
                print(f"[WARMUP-ERR] id={q['id']:>2} name={q['name']}: {str(e)[:100]}")

    print("\n预热完成。\n")


def run_sqls(conn, sqls: List[Dict[str, Any]], repeat: int, only_ids: List[int],
             timeout: int, list_only: bool, csv_path: str):
    """执行查询并收集统计"""
    cur = conn.cursor()

    # 设置超时
    try:
        cur.execute(f"SET query_timeout = {timeout}")
    except Exception as e:
        print(f"警告：设置 query_timeout 失败: {e}")

    selected = [q for q in sqls if (not only_ids or q["id"] in only_ids)]

    if list_only:
        print("将执行的 SQL 列表：\n")
        for q in selected:
            print(f"{'=' * 80}")
            print(f"[{q['id']}] {q['name']}")
            print(f"{'=' * 80}")
            print(q['sql'].strip())
            print()
        return

    rows: List[List[Any]] = []
    print(f"共 {len(selected)} 条 SQL，每条重复 {repeat} 次。\n")

    # 记录总开始时间
    total_start = time.perf_counter()
    total_queries = 0
    total_success = 0
    total_failed = 0

    for q in selected:
        for r in range(1, repeat + 1):
            total_queries += 1
            try:
                start = time.perf_counter()
                cur.execute(q["sql"])
                result_count = len(cur.fetchall())  # 获取结果行数
                elapsed = time.perf_counter() - start
                print(f"[OK] id={q['id']:>2} rep={r:>2}/{repeat} rows={result_count:>6} "
                      f"time={elapsed:.3f}s name={q['name']}")
                rows.append([q["id"], q["name"], r, f"{elapsed:.6f}", result_count])
                total_success += 1
            except Exception as e:
                elapsed = time.perf_counter() - start
                error_msg = str(e)[:200]
                print(f"[FAIL] id={q['id']:>2} rep={r:>2}/{repeat} time={elapsed:.3f}s "
                      f"name={q['name']} error={error_msg}")
                rows.append([q["id"], q["name"], r, f"{elapsed:.6f}", f"ERROR: {error_msg}"])
                total_failed += 1

    # 计算总耗时
    total_elapsed = time.perf_counter() - total_start

    # 汇总统计
    by_id: Dict[int, List[float]] = {}
    all_times: List[float] = []  # 所有查询的耗时

    for qid, _, _, t, _ in rows:
        try:
            time_val = float(t)
            by_id.setdefault(qid, []).append(time_val)
            all_times.append(time_val)
        except ValueError:
            pass  # 跳过错误的行

    print("\n" + "=" * 80)
    print("==== 汇总统计 ====")
    print("=" * 80)
    for q in selected:
        ts = by_id.get(q["id"], [])
        if not ts:
            print(f"[{q['id']:>2}] {q['name']}: 无有效数据")
            continue
        avg = sum(ts) / len(ts)
        p50 = percentile(ts, 50)
        p95 = percentile(ts, 95)
        p99 = percentile(ts, 99)
        min_t = min(ts)
        max_t = max(ts)
        print(f"[{q['id']:>2}] {q['name']}")
        print(f"      min={min_t:.3f}s avg={avg:.3f}s p50={p50:.3f}s "
              f"p95={p95:.3f}s p99={p99:.3f}s max={max_t:.3f}s runs={len(ts)}")

    # 打印总体统计
    print("\n" + "=" * 80)
    print("==== 总体统计 ====")
    print("=" * 80)
    print(f"总查询数:     {total_queries}")
    print(f"成功:         {total_success}")
    print(f"失败:         {total_failed}")
    print(f"总耗时:       {total_elapsed:.3f}s ({total_elapsed / 60:.2f} 分钟)")
    if all_times:
        print(f"所有查询累计: {sum(all_times):.3f}s")
        print(f"平均每次:     {sum(all_times) / len(all_times):.3f}s")
    print(f"吞吐量:       {total_queries / total_elapsed:.2f} 查询/秒")
    print("=" * 80)

    # 写入 CSV
    if csv_path:
        try:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["id", "name", "repeat", "seconds", "result_rows"])
                w.writerows(rows)
                # 在 CSV 末尾添加汇总行
                w.writerow([])
                w.writerow(["总查询数", total_queries, "", "", ""])
                w.writerow(["成功", total_success, "", "", ""])
                w.writerow(["失败", total_failed, "", "", ""])
                w.writerow(["总耗时(秒)", f"{total_elapsed:.3f}", "", "", ""])
                w.writerow(["累计查询时间(秒)", f"{sum(all_times):.3f}" if all_times else "0", "", "", ""])
            print(f"\n已写出明细: {csv_path}")
        except Exception as e:
            print(f"\n写入 CSV 失败: {e}")


def main():
    ap = argparse.ArgumentParser(
        description="StarRocks 查询压测脚本（复杂版，全量 + JOIN）",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--host", required=True, help="FE 主机地址")
    ap.add_argument("--port", type=int, default=9030, help="FE MySQL 端口（默认 9030）")
    ap.add_argument("--user", required=True, help="数据库用户名")
    ap.add_argument("--password", required=True, help="数据库密码")
    ap.add_argument("--db", required=True, help="数据库名")
    ap.add_argument("--table", default="binance_futures_trades_raw_ssd",
                    help="表名（默认 binance_futures_trades_raw_ssd）")
    ap.add_argument("--repeat", type=int, default=1, help="每条 SQL 重复次数")
    ap.add_argument("--warmup", type=int, default=0, help="预热次数（每条 SQL 先跑 N 次但不计入统计）")
    ap.add_argument("--timeout", type=int, default=1800, help="查询超时时间（秒）")
    ap.add_argument("--only", default="", help="仅执行这些 id，逗号分隔，如 1,3,5")
    ap.add_argument("--output", default="", help="若指定则把明细写到 CSV")
    ap.add_argument("--list", action="store_true", help="只列出 SQL 不执行")
    args = ap.parse_args()

    # 解析 only 参数
    only_ids = []
    if args.only:
        try:
            only_ids = [int(x.strip()) for x in args.only.split(",") if x.strip()]
        except ValueError:
            print(f"错误：--only 参数格式不正确: {args.only}", file=sys.stderr)
            sys.exit(1)

    # 生成 SQL 列表
    sqls = get_sqls(args.table)

    print("=" * 80)
    print(f"StarRocks 查询压测")
    print("=" * 80)
    print(f"目标: {args.host}:{args.port} / {args.db}.{args.table}")
    print(f"用户: {args.user}")
    print(f"重复: {args.repeat} 次 | 预热: {args.warmup} 次 | 超时: {args.timeout}s")
    if only_ids:
        print(f"仅执行: {only_ids}")
    print("=" * 80 + "\n")

    # 连接数据库
    try:
        conn = pymysql.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.db,
            autocommit=True,
            connect_timeout=30,
            read_timeout=args.timeout,
            write_timeout=args.timeout
        )
        print(f"✓ 数据库连接成功\n")
    except Exception as e:
        print(f"✗ 数据库连接失败: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        # 预热
        run_warmup(conn, sqls, args.warmup, only_ids)

        # 正式测试
        run_sqls(conn, sqls, args.repeat, only_ids, args.timeout, args.list, args.output)
    except KeyboardInterrupt:
        print("\n\n用户中断")
    except Exception as e:
        print(f"\n执行出错: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        print("\n数据库连接已关闭")


if __name__ == "__main__":
    main()