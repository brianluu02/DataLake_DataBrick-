# Databricks notebook source
# Databricks Notebook: 00_silver_helper_utils
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit, max as spark_max

def get_watermark_from_meta(catalog_schema_meta, asset_key, default_value="1900-01-01 00:00:00"):
    """Lấy giá trị Watermark cũ từ bảng metadata"""
    meta_table = f"{catalog_schema_meta}.etl_watermarks"
    
    # Tạo bảng metadata lưu watermark nếu chưa tồn tại
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema_meta.split('.')[0]}.{catalog_schema_meta.split('.')[1]}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {meta_table} (
            asset_key STRING,
            last_watermark TIMESTAMP
        ) USING DELTA
    """)
    
    # Tìm watermark cũ
    watermark_df = spark.table(meta_table).filter(col("asset_key") == asset_key)
    if watermark_df.count() > 0:
        return watermark_df.collect()[0]["last_watermark"]
    return default_value

def update_watermark_meta(catalog_schema_meta, asset_key, new_watermark):
    """Cập nhật giá trị Watermark mới vào bảng metadata (Upsert/Merge)"""
    meta_table = f"{catalog_schema_meta}.etl_watermarks"
    
    # Thực hiện MERGE để cập nhật hoặc chèn mới watermark
    spark.sql(f"""
        MERGE INTO {meta_table} AS target
        USING (SELECT '{asset_key}' AS asset_key, CAST('{new_watermark}' AS TIMESTAMP) AS last_watermark) AS source
        ON target.asset_key = source.asset_key
        WHEN MATCHED THEN UPDATE SET target.last_watermark = source.last_watermark
        WHEN NOT MATCHED THEN INSERT (asset_key, last_watermark) VALUES (source.asset_key, source.last_watermark)
    """)

def process_staging_load(
    source_table: str,
    target_table: str,
    asset_key: str,
    catalog_schema_meta: str = "data_dev_olist.silver",
    watermark_col: str = "last_update"
):
    """
    Hàm xử lý Incremental Load từ Bronze Table sang Silver Table trên Databricks
    """
    print(f"🔄 Bắt đầu xử lý: {source_table} -> {target_table}")
    
    # 1. Đọc dữ liệu từ bảng Bronze (Đã là Delta Table trong Unity Catalog)
    if not spark.catalog.tableExists(source_table):
        print(f"⚠️ Bảng nguồn {source_table} không tồn tại hoặc trống. Bỏ qua.")
        return

    spark_df = spark.table(source_table)
    total_input_rows = spark_df.count()
    
    if total_input_rows == 0:
        print(f"⚠️ Bảng nguồn {source_table} không có dữ liệu. Bỏ qua.")
        return

    # Ép kiểu cột watermark về Timestamp nếu có
    if watermark_col in spark_df.columns:
        spark_df = spark_df.withColumn(watermark_col, to_timestamp(col(watermark_col)))
    else:
        print(f"⚠️ Không tìm thấy cột watermark '{watermark_col}' trong {source_table}. Chạy Full Load / Không filter.")

    # 2. Lấy Low Watermark & Lọc dữ liệu mới
    low_watermark = get_watermark_from_meta(catalog_schema_meta, asset_key)
    print(f"💧 Low Watermark hiện tại: {low_watermark}")

    if watermark_col in spark_df.columns:
        spark_df = spark_df.filter(col(watermark_col) > lit(low_watermark))

    filtered_count = spark_df.count()
    print(f"🔍 Số lượng dòng mới cần xử lý: {filtered_count} (Bỏ qua {total_input_rows - filtered_count} dòng cũ)")

    if filtered_count == 0:
        print(f"✅ Không có dữ liệu mới cho {asset_key}. Hoàn thành.")
        return

    # 3. Tính toán New Watermark dựa trên batch dữ liệu mới
    new_batch_watermark = None
    if watermark_col in spark_df.columns:
        try:
            new_batch_watermark = spark_df.agg(spark_max(watermark_col)).collect()[0][0]
        except Exception as e:
            print(f"⚠️ Không thể tính toán max watermark mới: {e}")

    # 4. Thêm các cột Audit Log hệ thống Databricks
    # Lấy job_run_id từ context của Databricks Workflow nếu có, nếu chạy tay thì để mặc định "manual"
    import json
    context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    context_obj = json.loads(context_str)
    run_id = context_obj.get("currentRunId", {}).get("id", "manual")

    spark_df = spark_df.withColumn("_ingested_at", current_timestamp()) \
                       .withColumn("_job_run_id", lit(str(run_id)))

    # 5. Ghi dữ liệu vào bảng Silver dưới dạng APPEND (Do đây là mô hình lưu lịch sử Staging History)
    # Tự tạo schema và bảng nếu chạy lần đầu
    target_schema = ".".join(target_table.split(".")[:2])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    
    (spark_df.write
     .format("delta")
     .mode("append")
     .saveAsTable(target_table))

    # 6. Cập nhật New Watermark vào metadata sau khi đã ghi thành công
    if new_batch_watermark:
        print(f"💾 Lưu watermark mới vào metadata: {new_batch_watermark}")
        update_watermark_meta(catalog_schema_meta, asset_key, new_batch_watermark)
        
    print(f"✅ Đã xử lý xong bảng: {target_table}\n" + "="*50)

# COMMAND ----------

# MAGIC %md
# MAGIC oke 1 
# MAGIC

# COMMAND ----------

# Databricks Notebook: 01_silver_staging_pipeline

# 1. Gọi hàm helper từ Notebook đã tạo ở Bước 1
# (Giả sử cả 2 notebook nằm chung một thư mục)
# %run ./00_silver_helper_utils

# 2. Cấu hình Catalog và Schema mục tiêu trong Unity Catalog
CATALOG = "data_dev_olist"
META_SCHEMA = f"{CATALOG}.silver"

# 3. Danh sách cấu hình mapping 9 bảng từ Bronze sang Silver
pipeline_configs = [
    {"source": "customer", "watermark": "last_update"},
    {"source": "seller", "watermark": "last_update"},
    {"source": "product", "watermark": "last_update"},
    {"source": "order", "watermark": "last_update"}, # Có thể sửa thành order_purchase_timestamp nếu cần
    {"source": "order_item", "watermark": "last_update"},
    {"source": "payment", "watermark": "last_update"},
    {"source": "order_review", "watermark": "last_update"},
    {"source": "product_category", "watermark": "last_update"},
    {"source": "geolocation", "watermark": "last_update"}
]

# 4. Thực hiện chạy vòng lặp xử lý toàn bộ các bảng tự động
for config in pipeline_configs:
    source_tbl = f"{CATALOG}.bronze.{config['source']}"
    target_tbl = f"{CATALOG}.silver.stg_{config['source']}"
    asset_key = f"stg_{config['source']}"
    
    process_staging_load(
        source_table=source_tbl,
        target_table=target_tbl,
        asset_key=asset_key,
        catalog_schema_meta=META_SCHEMA,
        watermark_col=config["watermark"]
    )

print("🎉 Hoàn thành dịch chuyển và xử lý toàn bộ tầng Silver Staging trên Databricks!")

# COMMAND ----------

# MAGIC %md
# MAGIC oke 2 
# MAGIC

# COMMAND ----------

# Databricks Notebook: 00_silver_cleaned_utils
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, max as spark_max

# ==============================================================================
# 1. QUẢN LÝ WATERMARK (Lưu vết mốc thời gian đã xử lý)
# ==============================================================================
def get_watermark_from_meta(catalog_schema_meta, asset_key, default_value="1900-01-01 00:00:00"):
    meta_table = f"{catalog_schema_meta}.etl_watermarks"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {'.'.join(catalog_schema_meta.split('.')[:2])}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {meta_table} (
            asset_key STRING,
            last_watermark TIMESTAMP
        ) USING DELTA
    """)
    watermark_df = spark.table(meta_table).filter(col("asset_key") == asset_key)
    if watermark_df.count() > 0:
        return watermark_df.collect()[0]["last_watermark"]
    return default_value

def update_watermark_meta(catalog_schema_meta, asset_key, new_watermark):
    meta_table = f"{catalog_schema_meta}.etl_watermarks"
    spark.sql(f"""
        MERGE INTO {meta_table} AS target
        USING (SELECT '{asset_key}' AS asset_key, CAST('{new_watermark}' AS TIMESTAMP) AS last_watermark) AS source
        ON target.asset_key = source.asset_key
        WHEN MATCHED THEN UPDATE SET target.last_watermark = source.last_watermark
        WHEN NOT MATCHED THEN INSERT (asset_key, last_watermark) VALUES (source.asset_key, source.last_watermark)
    """)

# ==============================================================================
# 2. CORE PROCESSOR: ĐỌC VOLUME TỪ AZURE -> MERGE SILVER DELTA
# ==============================================================================
def process_silver_asset_from_volume(
    volume_file_path: str,    # Đường dẫn file CSV trên Azure Volume
    target_table: str,        # Tên bảng Delta đích (catalog.schema.table)
    asset_key: str,
    merge_key: str,
    transform_func=None,
    watermark_col: str = "last_update" # Cột mốc thời gian để chạy Incremental
):
    print(f"🚀 [START] Processing: {asset_key.upper()}")
    catalog_schema_meta = ".".join(target_table.split(".")[:2])

    # --- BƯỚC 1: ĐỌC DỮ LIỆU TỪ VOLUME ---
    try:
        raw_df = (spark.read
                  .format("csv")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .load(volume_file_path))
    except Exception as e:
        print(f"❌ Không tìm thấy hoặc không thể đọc file: {volume_file_path}. Lỗi: {str(e)}")
        return

    total_input = raw_df.count()
    if total_input == 0:
        print(f"💤 File nguồn trống. Bỏ qua.")
        return

    # Chuẩn hóa kiểu dữ liệu cột watermark về Timestamp
    if watermark_col in raw_df.columns:
        raw_df = raw_df.withColumn(watermark_col, to_timestamp(col(watermark_col)))
    
    # --- BƯỚC 2: LỌC INCREMENTAL ---
    high_watermark = get_watermark_from_meta(catalog_schema_meta, asset_key)
    print(f"🕵️ Low Watermark: {high_watermark}")
    
    if watermark_col in raw_df.columns:
        incremental_df = raw_df.filter(col(watermark_col) > lit(high_watermark))
    else:
        print(f"⚠️ Không có cột '{watermark_col}'. Tự động chuyển sang Full Load.")
        incremental_df = raw_df

    batch_count = incremental_df.count()
    print(f"🔍 Số lượng dòng mới cần nạp: {batch_count} dòng.")

    # --- BƯỚC 3: TRANSFORM & MERGE INTO DELTA LAKE ---
    if batch_count > 0:
        if transform_func:
            incremental_df = transform_func(incremental_df)
            
        # Thêm cột mốc thời gian nạp hệ thống Databricks
        incremental_df = incremental_df.withColumn("_ingested_at", current_timestamp()) \
                                       .withColumn("is_active", lit(True)) # 👈 THÊM DÒNG NÀY VÀO ĐÂY

        # Tìm mốc watermark lớn nhất của batch này để lưu lại
        new_batch_watermark = None
        if watermark_col in incremental_df.columns:
            new_batch_watermark = incremental_df.agg(spark_max(watermark_col)).collect()[0][0]

        # Thực hiện ghi/cập nhật vào bảng Delta
        if not spark.catalog.tableExists(target_table):
            print(f"✨ Khởi tạo bảng Delta mới ở tầng Silver: {target_table}")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema_meta}")
            (incremental_df.write.format("delta").mode("overwrite").saveAsTable(target_table))
        else:
            print(f"📦 Đang Upsert (Merge) dữ liệu vào {target_table}...")
            # Gom cụm loại bỏ trùng lặp khóa chính ngay trong batch mới trước khi Merge
            from pyspark.sql.window import Window
            from pyspark.sql.functions import row_number
            
            order_col = watermark_col if watermark_col in incremental_df.columns else "_ingested_at"
            window_spec = Window.partitionBy(merge_key).orderBy(col(order_col).desc())
            deduped_batch_df = incremental_df.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")
            
            deduped_batch_df.createOrReplaceTempView("src_batch")
            spark.sql(f"""
                MERGE INTO {target_table} AS tgt
                USING src_batch AS src
                ON tgt.{merge_key} = src.{merge_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

        # Cập nhật metadata lưu vết cho lần chạy sau
        if new_batch_watermark:
            update_watermark_meta(catalog_schema_meta, asset_key, new_batch_watermark)
            print(f"💾 Lưu Watermark mới thành công: {new_batch_watermark}")
    else:
        print("💤 Không có dữ liệu mới phát sinh.")

    print(f"🏁 [FINISH] Kết thúc xử lý bảng {asset_key.upper()}\n" + "="*60)

# COMMAND ----------

# Databricks Notebook: 01_silver_cleaned_pipeline

# 1. Nhúng file utilities xử lý bên trên vào
# %run ./00_silver_cleaned_utils

import os
from pyspark.sql.functions import col, round, concat, md5

VOLUME_BASE = "/Volumes/data_dev_olist/bronze/raw"
CATALOG = "data_dev_olist"

# ==============================================================================
# LOGIC TRANSFORM CHO TỪNG FILE ĐẶC THÙ (Giữ nguyên logic PySpark cũ của bạn)
# ==============================================================================
def transform_customer(df): return df.na.drop(subset=["customer_id"])
def transform_seller(df): return df.na.drop(subset=["seller_id"])

def transform_product(df):
    df = df.na.drop(subset=["product_id"])
    cols_to_int = ["product_description_length", "product_photos_qty", "product_length_cm", "product_height_cm", "product_width_cm", "product_weight_g"]
    for c in cols_to_int:
        if c in df.columns: df = df.withColumn(c, col(c).cast("integer"))
    return df

def transform_order_item(df):
    return df.withColumn("price", round(col("price"), 2).cast("double")) \
             .withColumn("freight_value", round(col("freight_value"), 2).cast("double")).na.drop(subset=["order_item_id"])

def transform_payment(df):
    df = df.withColumn("payment_value", round(col("payment_value"), 2).cast("double"))
    if "payment_installments" in df.columns:
        df = df.withColumn("payment_installments", col("payment_installments").cast("integer"))
    df = df.withColumn("pk_hash", md5(concat(col("order_id"), col("payment_sequential"))))
    return df.na.drop(subset=["pk_hash"])

def transform_order_review(df):
    if "review_comment_title" in df.columns: df = df.drop("review_comment_title")
    return df.na.drop(subset=["review_id"])

def transform_order(df): return df.na.drop(subset=["order_id"])
def transform_product_category(df): return df.na.drop(subset=["product_category_name"])

def transform_geolocation(df):
    df = df.na.drop(subset=["geolocation_zip_code_prefix"])
    return df.filter((col("geolocation_lat") <= 5.27438888) & (col("geolocation_lng") >= -73.98283055) & 
                     (col("geolocation_lat") >= -33.75116944) & (col("geolocation_lng") <= -34.79314722))

# ==============================================================================
# ĐĂNG KÝ MAPPING 9 FILE THỰC TẾ TRÊN AZURE VOLUME
# ==============================================================================
pipeline_registry = [
    {"file": "olist_customers_dataset.csv", "tgt": "clean_customer", "key": "customer_id", "func": transform_customer, "wm_col": "last_update"},
    {"file": "olist_sellers_dataset.csv", "tgt": "clean_seller", "key": "seller_id", "func": transform_seller, "wm_col": "last_update"},
    {"file": "olist_products_dataset.csv", "tgt": "clean_product", "key": "product_id", "func": transform_product, "wm_col": "last_update"},
    {"file": "olist_order_items_dataset.csv", "tgt": "clean_order_item", "key": "order_item_id", "func": transform_order_item, "wm_col": "last_update"},
    {"file": "olist_order_payments_dataset.csv", "tgt": "clean_payment", "key": "pk_hash", "func": transform_payment, "wm_col": "last_update"},
    {"file": "olist_order_reviews_dataset.csv", "tgt": "clean_order_review", "key": "review_id", "func": transform_order_review, "wm_col": "last_update"},
    {"file": "olist_orders_dataset.csv", "tgt": "clean_order", "key": "order_id", "func": transform_order, "wm_col": "last_update"},
    {"file": "product_category_name_translation.csv", "tgt": "clean_product_category", "key": "product_category_name", "func": transform_product_category, "wm_col": "last_update"},
    {"file": "olist_geolocation_dataset.csv", "tgt": "clean_geolocation", "key": "geolocation_zip_code_prefix", "func": transform_geolocation, "wm_col": "last_update"}
]

# Thực thi quét qua toàn bộ danh sách
for asset in pipeline_registry:
    full_csv_path = os.path.join(VOLUME_BASE, asset["file"])
    target_delta_table = f"{CATALOG}.silver.{asset['tgt']}"
    
    process_silver_asset_from_volume(
        volume_file_path=full_csv_path,
        target_table=target_delta_table,
        asset_key=asset['tgt'],
        merge_key=asset['key'],
        transform_func=asset['func'],
        watermark_col=asset['wm_col']
    )

# ==============================================================================
# XỬ LÝ BẢNG PHÁT SINH: DATE DIMENSION
# ==============================================================================
print("📆 Đang trích xuất dữ liệu cho bảng Date Dimension...")
try:
    clean_order_tbl = f"{CATALOG}.silver.clean_order"
    date_dim_tbl = f"{CATALOG}.silver.date_dimension"
    
    if spark.catalog.tableExists(clean_order_tbl):
        date_df = spark.table(clean_order_tbl).select("order_purchase_timestamp").na.drop().distinct()
        (date_df.write.format("delta").mode("overwrite").saveAsTable(date_dim_tbl))
        print(f"✅ Đã đồng bộ thành công Date Dimension -> {date_dim_tbl}")
except Exception as e:
    print(f"❌ Lỗi xử lý bảng Date Dimension: {e}")

print("🎉 PIPELINE HOÀN THÀNH TOÀN BỘ TRÊN DATABRICKS!")