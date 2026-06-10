import os
import sys
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, desc, regexp_replace, trim, length

# ==========================================
# WINDOWS ENVIRONMENT OVERRIDES
# ==========================================
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ.get("PATH", "") + ";" + r"C:\hadoop\bin"


def process_users_bronze_to_silver(spark):
    print("\n==========================================")
    print(" 👤 PROCESSING USER PROFILES")
    print("==========================================")
    
    bronze_path = "data/raw/user_profiles/*/*.json"
    silver_path = "data/silver/user_interactions.parquet"
    
    if len(glob.glob(bronze_path)) == 0:
        print("❌ ERROR: No User JSON files found. Skipping.")
        return

    raw_df = spark.read.option("multiline", "true").json(bronze_path)
    latest_users_df = raw_df.orderBy(col("ingested_at").desc()).dropDuplicates(["steam_id"])
    
    exploded_df = latest_users_df.select(
        col("steam_id"),
        explode(col("games")).alias("game")
    )
    
    silver_df = exploded_df.select(
        col("steam_id"),
        col("game.appid").alias("app_id"),
        col("game.playtime_forever").alias("playtime_minutes")
    ).filter(col("playtime_minutes") > 0)

    print(f"💾 Writing clean Users to: {silver_path}")
    silver_df.write.mode("overwrite").parquet(silver_path)


def process_metadata_bronze_to_silver(spark):
    print("\n==========================================")
    print(" 🎮 PROCESSING APP METADATA")
    print("==========================================")
    
    bronze_path = "data/raw/app_metadata/*/*.json"
    silver_path = "data/silver/app_metadata.parquet"
    
    if len(glob.glob(bronze_path)) == 0:
        print("❌ ERROR: No Metadata JSON files found. Skipping.")
        return

    raw_df = spark.read.option("multiline", "true").json(bronze_path)
    latest_df = raw_df.orderBy(col("ingested_at").desc()).dropDuplicates(["app_id"])
    
    silver_df = latest_df.select(
        col("app_id"),
        col("metadata.name").alias("game_name"),
        col("metadata.is_free").alias("is_free"),
        col("metadata.price_overview.final_formatted").alias("price_formatted"),
        col("metadata.release_date.date").alias("release_date"),
        col("metadata.metacritic.score").alias("metacritic_score"),
        col("metadata.genres.description").alias("genres"),
        col("metadata.categories.description").alias("categories")
    )
    
    print(f"💾 Writing clean Metadata to: {silver_path}")
    silver_df.write.mode("overwrite").parquet(silver_path)


def process_reviews_bronze_to_silver(spark):
    print("\n==========================================")
    print(" 📝 PROCESSING APP REVIEWS (WITH TEXT CLEANING)")
    print("==========================================")
    
    bronze_path = "data/raw/app_reviews/*/*.json"
    silver_path = "data/silver/app_reviews.parquet"
    
    if len(glob.glob(bronze_path)) == 0:
        print("❌ ERROR: No Review JSON files found. Skipping.")
        return

    raw_df = spark.read.option("multiline", "true").json(bronze_path)
    latest_df = raw_df.orderBy(col("ingested_at").desc()).dropDuplicates(["app_id"])
    
    exploded_df = latest_df.select(
        col("app_id"),
        explode(col("reviews")).alias("review")
    )
    
    silver_df = exploded_df.select(
        col("app_id"),
        col("review.review_id").alias("review_id"),
        col("review.steam_id").alias("steam_id"),
        col("review.playtime_minutes").alias("playtime_minutes"),
        col("review.voted_up").alias("voted_up"),
        col("review.weighted_score").alias("weighted_score"),
        col("review.review_text").alias("review_text")
    )
    
    # -----------------------------------------------------
    # 🧼 THE TEXT CLEANING PIPELINE
    # -----------------------------------------------------
    print("🧼 Cleaning review text (removing linebreaks, extra spaces, and spam)...")
    
    # Replace all newlines (\n) and carriage returns (\r) with a single space
    clean_df = silver_df.withColumn(
        "review_text", 
        regexp_replace(col("review_text"), r"[\r\n]+", " ")
    )
    
    # Strip any leading or trailing whitespace
    clean_df = clean_df.withColumn(
        "review_text", 
        trim(col("review_text"))
    )
    
    # Drop the garbage. Require reviews to be at least 10 characters long.
    final_df = clean_df.filter(length(col("review_text")) >= 10)
    # -----------------------------------------------------

    print(f"💾 Writing clean Reviews to: {silver_path}")
    final_df.write.mode("overwrite").parquet(silver_path)


if __name__ == "__main__":
    os.makedirs("data/silver", exist_ok=True)
    
    print("🚀 Initializing PySpark Engine (Once for all tasks)...")
    
    # Initialize a local Spark session utilizing all available CPU cores
    spark = SparkSession.builder \
        .appName("Steam_Unified_Silver_Pipeline") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    # Run the entire pipeline sequentially using the same engine
    process_users_bronze_to_silver(spark)
    process_metadata_bronze_to_silver(spark)
    process_reviews_bronze_to_silver(spark)
    
    spark.stop()
    print("\n✅ ALL SILVER LAYER PIPELINES COMPLETE!")