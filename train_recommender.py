import os
import sys
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# ==========================================
# WINDOWS ENVIRONMENT OVERRIDES
# ==========================================
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ.get("PATH", "") + ";" + r"C:\hadoop\bin"


def train_als_model(spark):
    print("\n==========================================")
    print(" 🤖 TRAINING MACHINE LEARNING MODEL")
    print("==========================================")
    
    # 1. Load the Gold Matrix
    print("📥 Loading Gold Layer Matrix...")
    gold_df = spark.read.parquet("data/gold/ml_interaction_matrix.parquet")
    
    # 2. Train / Test Split
    # We hide 20% of the data from the model to test it like a final exam later
    print("🪓 Splitting Data (80% Training / 20% Testing)...")
    (training_data, test_data) = gold_df.randomSplit([0.8, 0.2], seed=42)

    # 3. Configure the ALS Algorithm
    print("⚙️  Configuring ALS Algorithm...")
    als = ALS(
        maxIter=10,               # How many times it loops over the data to learn
        regParam=0.1,             # Prevents "overfitting" (memorizing instead of learning)
        userCol="user_index",
        itemCol="item_index",
        ratingCol="interaction_score",
        coldStartStrategy="drop", # CRITICAL: Drops users in the test set it has never seen before
        nonnegative=True          # Ensures it doesn't predict negative playtime/scores
    )

    # 4. Train the Model
    print("🧠 Training the Model (This might take a minute)...")
    model = als.fit(training_data)

    # 5. Evaluate the Model (The "Final Exam")
    print("📊 Evaluating Model Accuracy...")
    predictions = model.transform(test_data)
    
    evaluator = RegressionEvaluator(
        metricName="rmse", # Root Mean Squared Error (Lower is better)
        labelCol="interaction_score",
        predictionCol="prediction"
    )
    
    rmse = evaluator.evaluate(predictions)
    print(f"🎯 Root-mean-square error (RMSE) = {rmse:.4f}")

    # 6. Save the Trained Model
    print("💾 Saving the trained model for production inference...")
    model.write().overwrite().save("data/models/als_steam_recommender")
    
    print("\n✅ MODEL TRAINING COMPLETE!")

if __name__ == "__main__":
    os.makedirs("data/models", exist_ok=True)
    
    spark = SparkSession.builder \
        .appName("Steam_ALS_Training") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    train_als_model(spark)
    
    spark.stop()