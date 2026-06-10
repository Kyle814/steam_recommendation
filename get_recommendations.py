import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.feature import StringIndexerModel, IndexToString

# ==========================================
# WINDOWS ENVIRONMENT OVERRIDES
# ==========================================
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ.get("PATH", "") + ";" + r"C:\hadoop\bin"


def get_user_recommendations(spark, target_steam_id, num_recs=5):
    print(f"\n==========================================")
    print(f" 🎯 GENERATING RECOMMENDATIONS FOR: {target_steam_id}")
    print(f"==========================================\n")

    # 1. Load the Models and Metadata
    print("🧠 Booting up the Machine Learning Model...")
    als_model = ALSModel.load("data/models/als_steam_recommender")
    user_indexer = StringIndexerModel.load("data/models/user_indexer")
    item_indexer = StringIndexerModel.load("data/models/item_indexer")
    
    # We only need the ID, Name, and Genres from the metadata to print a clean table
    metadata_df = spark.read.parquet("data/silver/app_metadata.parquet") \
        .select("app_id", "game_name", "genres")

    # 2. Package the Target User
    # We create a tiny DataFrame containing just the single user ID we want to query
    user_df = spark.createDataFrame([(target_steam_id,)], ["steam_id"])

    # 3. Translate String ID to ML Integer Index
    # Note: handleInvalid="skip" ensures it won't crash if it doesn't recognize the user
    user_indexer.setHandleInvalid("skip")
    user_indexed_df = user_indexer.transform(user_df)

    if user_indexed_df.count() == 0:
        print(f"❌ ERROR: User {target_steam_id} was not found in the training data.")
        print("They either do not exist, or they had 0 playtime and were filtered out.")
        return

    # 4. Ask the Model for Recommendations
    print("🔍 Calculating optimal game matches...")
    raw_recommendations = als_model.recommendForUserSubset(user_indexed_df, num_recs)

    # 5. Unpack the Results
    # The model returns an array of arrays, so we have to explode it to make it a flat table
    flat_recs = raw_recommendations.select(
        col("user_index"),
        explode(col("recommendations")).alias("rec")
    ).select(
        col("rec.item_index").alias("item_index"),
        col("rec.rating").alias("predicted_affinity_score")
    )

    # 6. Translate ML Item Integers back to App IDs
    print("🔤 Translating Game IDs to Human Readable Names...")
    item_converter = IndexToString(
        inputCol="item_index", 
        outputCol="app_id", 
        labels=item_indexer.labels
    )
    translated_recs_df = item_converter.transform(flat_recs)

    # 7. Join with Metadata to get the Game Name
    final_recs_df = translated_recs_df.join(metadata_df, on="app_id", how="left") \
        .orderBy(col("predicted_affinity_score").desc())

    # 8. Print the Final Dashboard
    print("\n✅ SUCCESS! Here are the recommended games:")
    final_recs_df.select("game_name", "genres", "predicted_affinity_score").show(truncate=False)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Steam_Recommender_Inference") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    # 👇 CHANGE THIS TO A REAL STEAM ID FROM YOUR JSON FILES
    TARGET_STEAM_ID = "76561197963074068" 
    
    get_user_recommendations(spark, TARGET_STEAM_ID, num_recs=5)
    
    spark.stop()