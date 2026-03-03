from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp, lit, rand, when
from datetime import datetime, timedelta
import time

# 配置参数
START_DATE = "2025-01-20"
RECORDS_PER_DAY = 12300000  # 每天1230万条数据
BATCH_SIZE = 123000001  # 每批次100万条,分批写入减少内存压力


def create_spark_session():
    """创建Spark会话"""
    return (
        SparkSession.builder
        .appName("WriteIcebergDaily")
        .config("spark.sql.catalog.iceberg_hms", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_hms.type", "hive")
        .config("spark.sql.catalog.iceberg_hms.uri", "thrift://127.0.0.1:9083")
        .config("spark.sql.catalog.iceberg_hms.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg_hms.warehouse", "s3a://sr-prod/warehouse")
        .config("spark.sql.catalog.iceberg_hms.s3.endpoint", "http://10.88.0.75:30080")
        .config("spark.sql.catalog.iceberg_hms.s3.region", "RegionOne")
        .config("spark.sql.catalog.iceberg_hms.s3.access-key-id", "AYCSAC4N91YHRBLYG8TA")
        .config("spark.sql.catalog.iceberg_hms.s3.secret-access-key", "fhS8tlNXemXs21MdIA2H11qX5qgWpdBSObsHYjuV")
        # 优化配置
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        .getOrCreate()
    )


def generate_daily_data(spark, date_str, num_records, batch_size):
    """
    生成指定日期的数据并分批写入

    Args:
        spark: SparkSession对象
        date_str: 日期字符串,格式"YYYY-MM-DD"
        num_records: 总记录数
        batch_size: 每批次记录数
    """
    print(f"\n开始处理日期: {date_str}")
    print(f"总记录数: {num_records:,}, 批次大小: {batch_size:,}")

    # 计算批次数
    num_batches = (num_records + batch_size - 1) // batch_size

    # 交易对列表
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"]
    exchanges = ["binance-futures"]
    sides = ["buy", "sell"]

    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    start_ts_ns = int(date_obj.timestamp() * 1e9)

    total_written = 0

    for batch_idx in range(num_batches):
        batch_start = time.time()

        # 计算本批次的记录数
        current_batch_size = min(batch_size, num_records - total_written)

        # 生成基础数据范围
        df_range = spark.range(current_batch_size)

        # 先计算ID偏移量
        id_offset = batch_idx * batch_size

        # 生成随机数据
        df = (df_range
              .withColumn("id", (lit(id_offset) + df_range.id).cast("long"))
              .withColumn("symbol",
                          when(rand() < 0.4, lit("BTCUSDT"))
                          .when(rand() < 0.3, lit("ETHUSDT"))
                          .when(rand() < 0.2, lit("BNBUSDT"))
                          .when(rand() < 0.05, lit("SOLUSDT"))
                          .otherwise(lit("ADAUSDT")))
              .withColumn("exchange", lit("binance-futures"))
              .withColumn("exchange_ts_ns",
                          (lit(start_ts_ns) + (rand() * 86400 * 1e9).cast("long")))
              .withColumn("local_ts_ns",
                          (lit(start_ts_ns) + (rand() * 86400 * 1e9).cast("long")))
              .withColumn("price",
                          when((rand() * 5).cast("int") == 0, lit(43000.0) + (rand() * 2000))
                          .when((rand() * 5).cast("int") == 1, lit(2300.0) + (rand() * 200))
                          .when((rand() * 5).cast("int") == 2, lit(310.0) + (rand() * 30))
                          .when((rand() * 5).cast("int") == 3, lit(102.0) + (rand() * 20))
                          .otherwise(lit(0.58) + (rand() * 0.1)))
              .withColumn("quantity", rand() * 10)
              .withColumn("side",
                          when(rand() < 0.5, lit("buy")).otherwise(lit("sell")))
              .withColumn("event_date", to_date(lit(date_str)))
              .withColumn("etl_time", to_timestamp(lit(f"{date_str} 12:00:00")))
              .select("symbol", "id", "exchange", "exchange_ts_ns", "local_ts_ns",
                      "price", "quantity", "side", "event_date", "etl_time")
              )

        # 写入Iceberg表
        df.writeTo("iceberg_hms.test.binance_futures_trades").append()

        total_written += current_batch_size
        batch_elapsed = time.time() - batch_start

        print(f"  批次 {batch_idx + 1}/{num_batches} 完成: "
              f"写入 {current_batch_size:,} 条记录, "
              f"累计 {total_written:,}/{num_records:,}, "
              f"耗时 {batch_elapsed:.2f}秒")


    print(f"日期 {date_str} 处理完成,共写入 {total_written:,} 条记录\n")


def main():
    """主函数"""
    print("=" * 80)
    print("开始执行Iceberg数据写入任务")
    print("=" * 80)

    start_time = time.time()

    # 创建Spark会话
    spark = create_spark_session()

    try:
        # 解析起始日期
        start_date = datetime.strptime(START_DATE, "%Y-%m-%d")

        # 获取需要生成数据的天数(可以根据需要修改)
        # 这里默认生成30天的数据,可以根据实际需求调整
        num_days = 30

        print(f"配置信息:")
        print(f"  起始日期: {START_DATE}")
        print(f"  生成天数: {num_days}")
        print(f"  每天记录数: {RECORDS_PER_DAY:,}")
        print(f"  每批次大小: {BATCH_SIZE:,}")
        print(f"  总记录数: {RECORDS_PER_DAY * num_days:,}")
        print("=" * 80)

        # 逐天生成和写入数据
        for day_offset in range(num_days):
            current_date = start_date + timedelta(days=day_offset)
            date_str = current_date.strftime("%Y-%m-%d")

            day_start = time.time()
            generate_daily_data(spark, date_str, RECORDS_PER_DAY, BATCH_SIZE)
            day_elapsed = time.time() - day_start

            print(f"✓ 日期 {date_str} 总耗时: {day_elapsed:.2f}秒 "
                  f"({day_elapsed / 60:.2f}分钟)")



        total_elapsed = time.time() - start_time
        print("=" * 80)
        print(f"所有数据写入完成!")
        print(f"总耗时: {total_elapsed:.2f}秒 ({total_elapsed / 60:.2f}分钟)")
        print(f"总记录数: {RECORDS_PER_DAY * num_days:,}")
        print(f"平均速度: {(RECORDS_PER_DAY * num_days) / total_elapsed:.0f} 条/秒")
        print("=" * 80)

    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("Spark会话已关闭")


if __name__ == "__main__":
    main()