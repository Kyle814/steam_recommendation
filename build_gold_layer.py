import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, log1p
from pyspark.ml.feature import StringIndexer

# ==========================================
# WINDOWS ENVIRONMENT OVERRIDES
# ==========================================
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ.get("PATH", "") + ";" + r"C:\hadoop\bin"


def build_ml_interaction_matrix(spark):
    print("\n==========================================")
    print(" 🏆 BUILDING GOLD LAYER (ML MATRIX)")
    print("==========================================")
    
    # 1. Load Silver Tables
    print("📥 Loading Silver Tables...")
    users_df = spark.read.parquet("data/silver/user_interactions.parquet")
    reviews_df = spark.read.parquet("data/silver/app_reviews.parquet")
    
    # Drop the playtime column from reviews so it doesn't collide with the users table
    reviews_clean = reviews_df.select("steam_id", "app_id", "voted_up")

    # 2. Join the Data (Left Join ensures we keep games that have no reviews)
    print("🔗 Joining Reviews to User Interactions...")
    joined_df = users_df.join(
        reviews_clean,
        on=["steam_id", "app_id"],
        how="left"
    )

    # 3. Calculate the Hybrid Interaction Score
    print("🧮 Calculating Hybrid Interaction Scores (Log Scale + Review Bonus)...")
    
    # Apply log1p (log(playtime + 1)) to flatten massive outliers
    scored_df = joined_df.withColumn("base_score", log1p(col("playtime_minutes")))
    
    # Apply the Review Bonus Multipliers
    final_scored_df = scored_df.withColumn(
        "interaction_score",
        when(col("voted_up") == True, col("base_score") * 1.5)
        .when(col("voted_up") == False, col("base_score") * 0.5)
        .otherwise(col("base_score")) # No review = keep base score
    )

    # 4. Convert Strings to Integers for the ML Algorithm
    print("🔢 Indexing String IDs to ML-Ready Integers...")
    
    user_indexer = StringIndexer(inputCol="steam_id", outputCol="user_index", handleInvalid="skip")
    item_indexer = StringIndexer(inputCol="app_id", outputCol="item_index", handleInvalid="skip")
    
    # Fit the indexers to the data
    user_indexer_model = user_indexer.fit(final_scored_df)
    indexed_users_df = user_indexer_model.transform(final_scored_df)
    
    item_indexer_model = item_indexer.fit(indexed_users_df)
    gold_df = item_indexer_model.transform(indexed_users_df)

    # 5. Select the Final 3 Columns required for ALS
    final_matrix = gold_df.select(
        col("user_index").cast("integer"),
        col("item_index").cast("integer"),
        col("interaction_score").cast("float")
    )

    # 6. Save the Output and the Models
    print("💾 Saving Gold Matrix and Indexer Models...")
    
    # Save the Data
    final_matrix.write.mode("overwrite").parquet("data/gold/ml_interaction_matrix.parquet")
    
    # Save the Indexer Models (CRITICAL: We need these later to translate the predictions!)
    user_indexer_model.write().overwrite().save("data/models/user_indexer")
    item_indexer_model.write().overwrite().save("data/models/item_indexer")
    
    print("\n✅ GOLD LAYER COMPLETE! Final ML Matrix Preview:")
    final_matrix.show(10)

if __name__ == "__main__":
    os.makedirs("data/gold", exist_ok=True)
    os.makedirs("data/models", exist_ok=True)
    
    spark = SparkSession.builder \
        .appName("Steam_Gold_Layer") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    build_ml_interaction_matrix(spark)
    
    spark.stop()