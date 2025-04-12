from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MusicStreamingAnalysis") \
    .getOrCreate()

# Load datasets
logs_df = spark.read.option("header", True).csv("listening_logs.csv")
songs_df = spark.read.option("header", True).csv("songs_metadata.csv")

# Convert types
logs_df = logs_df.withColumn("timestamp", to_timestamp("timestamp")) \
                 .withColumn("duration_sec", col("duration_sec").cast("int"))

# Join logs with metadata
enriched_df = logs_df.join(songs_df, on="song_id")
enriched_df.cache()

# 1. User's Favorite Genre
user_genre_counts = enriched_df.groupBy("user_id", "genre").count()
fav_genre = user_genre_counts.withColumn("rank", row_number().over(
    Window.partitionBy("user_id").orderBy(desc("count")))) \
    .filter(col("rank") == 1).drop("rank")
fav_genre.write.csv("output/user_favorite_genres", header=True)

# 2. Average Listen Time Per Song
avg_listen_time = logs_df.groupBy("song_id") \
    .agg(avg("duration_sec").alias("avg_duration_sec"))
avg_listen_time.write.csv("output/avg_listen_time_per_song", header=True)

# 3. Top 10 Most Played Songs This Week
today = datetime.now()
week_ago = today - timedelta(days=7)
top_songs_week = logs_df.filter(col("timestamp") >= lit(week_ago.strftime("%Y-%m-%d"))) \
    .groupBy("song_id").count() \
    .orderBy(desc("count")) \
    .limit(10)
top_songs_week.write.csv("output/top_songs_this_week", header=True)

# 4. Recommend “Happy” Songs to “Sad” Listeners
user_genre_pref = enriched_df.groupBy("user_id", "genre").count()
sad_lovers = user_genre_pref.withColumn("rank", row_number().over(
    Window.partitionBy("user_id").orderBy(desc("count")))) \
    .filter((col("genre") == "Sad") & (col("rank") == 1)) \
    .select("user_id")

happy_songs = songs_df.filter(col("mood") == "Happy") \
    .select("song_id", "title").distinct()

user_song_history = logs_df.select("user_id", "song_id").distinct()

recommendations = sad_lovers.crossJoin(happy_songs) \
    .join(user_song_history, ["user_id", "song_id"], "left_anti") \
    .withColumn("row", row_number().over(Window.partitionBy("user_id").orderBy(monotonically_increasing_id()))) \
    .filter(col("row") <= 3) \
    .drop("row")

recommendations.write.csv("output/happy_recommendations", header=True)

# 5. Genre Loyalty Score
genre_counts = enriched_df.groupBy("user_id", "genre").count()
total_counts = enriched_df.groupBy("user_id").count().withColumnRenamed("count", "total")
max_genre_counts = genre_counts.withColumn("rank", row_number().over(
    Window.partitionBy("user_id").orderBy(desc("count")))) \
    .filter(col("rank") == 1).drop("rank")

loyalty_df = max_genre_counts.join(total_counts, "user_id") \
    .withColumn("loyalty_score", col("count") / col("total")) \
    .filter(col("loyalty_score") > 0.8)

loyalty_df.write.csv("output/genre_loyalty_scores", header=True)

# 6. Night Owl Users (12 AM - 5 AM)
night_logs = logs_df.withColumn("hour", hour("timestamp")) \
    .filter((col("hour") >= 0) & (col("hour") < 5))

night_owls = night_logs.groupBy("user_id").count().withColumnRenamed("count", "night_plays")
night_owls.write.csv("output/night_owl_users", header=True)

# 7. Save enriched logs
enriched_df.write.csv("output/enriched_logs", header=True)

spark.stop()
