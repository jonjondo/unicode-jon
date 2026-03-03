from clickhouse_driver import Client

# ====== 你的连接信息 ======
HOST = "cc-6we59fojaru89u568-ck-l2.clickhouseserver.japan.rds.aliyuncs.com"
PORT = 9000
USER = "reader"
PASSWORD = "J20Sc4Pch&todwEp"
DATABASE = "udata"

OUTPUT_FILE = "udata_schema.sql"


def main():
    client = Client(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )

    print("🔍 获取表列表...")

    tables = client.execute(
        f"""
        SELECT name
        FROM system.tables
        WHERE database = '{DATABASE}'
        AND engine NOT IN ('View')
        """
    )

    print(f"📦 共发现 {len(tables)} 张表")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for (table_name,) in tables:
            print(f"📄 导出表: {table_name}")

            create_sql = client.execute(
                f"SHOW CREATE TABLE {DATABASE}.{table_name}"
            )[0][0]

            f.write("-- ======================================\n")
            f.write(f"-- Table: {table_name}\n")
            f.write("-- ======================================\n")
            f.write(create_sql + ";\n\n")

    print(f"\n✅ 导出完成 -> {OUTPUT_FILE}")


if __name__ == "__main__":
    main()