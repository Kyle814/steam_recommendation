import os
import sys # <-- Add this import
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# 🔧 FIX 1: Stop PySpark from looking for 'python3' and use your active virtual environment


# 🔧 FIX 2: Pass the Java security flag secretly to avoid the Windows batch script crash

def process_user_bronze_to_silver():
    print("🚀 Initializing PySpark Session...")
    # Initialize a local Spark session utilizing all available CPU cores
    spark = SparkSession.builder \
        .appName("Steam_Bronze_to_Silver") \
        .master("local[*]") \
        .getOrCreate()
        
    # Suppress excessive Spark logging in the terminal
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Define Paths
    bronze_path = "data/raw/user_profiles/*.json"
    silver_path = "data/silver/user_interactions.parquet"
    
    print(f"📥 Reading raw JSON files from: {bronze_path}")
    
    # 2. Read all JSON files into a single DataFrame
    # PySpark automatically infers the schema from the JSON files
    raw_df = spark.read.json(bronze_path)
    
    print("⚙️ Flattening nested JSON arrays...")
    
    # 3. The Explode Transformation
    # 'games' is currently an array of dictionaries. 'explode' turns each 
    # game in the array into its own dedicated row, duplicating the steam_id.
    exploded_df = raw_df.select(
        col("steam_id"),
        explode(col("games")).alias("game")
    )
    
    # 4. Select and Flatten the final columns
    silver_df = exploded_df.select(
        col("steam_id"),
        col("game.appid").alias("app_id"),
        col("game.playtime_forever").alias("playtime_minutes")
    )
    
    # Optional: Filter out games with zero playtime (if the user bought it but never played it)
    silver_df = silver_df.filter(col("playtime_minutes") > 0)

    print(f"💾 Writing clean Parquet data to: {silver_path}")
    
    # 5. Write to Parquet
    # 'overwrite' ensures we can run this script multiple times without duplicating data
    silver_df.write.mode("overwrite").parquet(silver_path)
    
    # Print a quick preview of the transformed data
    print("\n✅ Transformation Complete! Silver Data Preview:")
    silver_df.show(10)
    
    spark.stop()

if __name__ == "__main__":
    # Ensure the Silver directory exists
    os.makedirs("data/silver", exist_ok=True)
    process_user_bronze_to_silver()