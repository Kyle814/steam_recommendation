import os
import sys
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, desc

# ==========================================
# WINDOWS ENVIRONMENT OVERRIDES
# ==========================================
# 1. Force PySpark to use the Python executable in your active virtual environment
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 2. Force PySpark to use Java 17 (Bypasses Java 25 crash)
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"

# 3. Point Spark to the Windows Hadoop translation binaries (Fixes file permission crash)
os.environ["HADOOP_HOME"] = r"C:\hadoop"

# 4. Add the bin folder to the system PATH so Java can execute hadoop.dll
os.environ["PATH"] = os.environ.get("PATH", "") + ";" + r"C:\hadoop\bin"


def process_user_bronze_to_silver():
    print("\n==========================================")
    print(" 🛠️  DIAGNOSTICS & SETUP")
    print("==========================================")
    
    # Check 1: Verify Working Directory
    print(f"📍 CURRENT DIRECTORY: {os.getcwd()}")
    
    # Check 2: Verify Data Exists (Notice the /*/ to read all date folders!)
    bronze_path = "data/raw/user_profiles/*/*.json"
    silver_path = "data/silver/user_interactions.parquet"
    
    json_files = glob.glob(bronze_path)
    print(f"📄 FOUND {len(json_files)} JSON FILES TO PROCESS.")
    
    if len(json_files) == 0:
        print("\n❌ ERROR: No JSON files found in data/raw/user_profiles/")
        print("Please ensure your working directory is correct and you have run the ingestion script.")
        return

    print("\n🚀 Initializing PySpark Session...")
    
    spark = SparkSession.builder \
        .appName("Steam_Bronze_to_Silver") \
        .master("local[*]") \
        .getOrCreate()
        
    
    spark.sparkContext.setLogLevel("ERROR")

    print(f"📥 Reading raw JSON files from: {bronze_path}")
    
  
    raw_df = spark.read.option("multiline", "true").json(bronze_path)
    
    print("🧹 Sorting by date and dropping older duplicate profiles...")
    
    latest_users_df = raw_df.orderBy(col("ingested_at").desc()).dropDuplicates(["steam_id"])
    
    print("⚙️  Flattening nested JSON arrays...")
    
    
    exploded_df = latest_users_df.select(
        col("steam_id"),
        explode(col("games")).alias("game")
    )
    
    silver_df = exploded_df.select(
        col("steam_id"),
        col("game.appid").alias("app_id"),
        col("game.playtime_forever").alias("playtime_minutes")
    )
    
    
    silver_df = silver_df.filter(col("playtime_minutes") > 0)

    print(f"💾 Writing clean Parquet data to: {silver_path}")
    

    silver_df.write.mode("overwrite").parquet(silver_path)
    
    # Print a quick preview of the transformed data
    print("\n✅ TRANSFORMATION COMPLETE! Silver Data Preview:")
    silver_df.show(10)
    
  
    spark.stop()

if __name__ == "__main__":
    # Ensure the Silver directory exists before attempting to write to it
    os.makedirs("data/silver", exist_ok=True)
    process_user_bronze_to_silver()