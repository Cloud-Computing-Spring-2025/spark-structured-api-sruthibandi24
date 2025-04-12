# 🎵 Music Listening Behavior Analysis using Spark

## 📘 Overview
This assignment analyzes user listening behavior and music trends using **Spark Structured APIs**. The goal is to extract insights from a fictional music streaming platform to understand genre preferences, song popularity, and listener engagement patterns.

We used **PySpark** to process two synthetic datasets:
- `listening_logs.csv`: Captures user listening activity.
- `songs_metadata.csv`: Contains metadata of the songs.

## 📂 Dataset Details

### `listening_logs.csv`
| Column       | Description                                  |
|--------------|----------------------------------------------|
| `user_id`    | Unique ID of the user                        |
| `song_id`    | Unique ID of the song                        |
| `timestamp`  | Datetime when the song was played            |
| `duration_sec` | Duration in seconds the song was played     |

### `songs_metadata.csv`
| Column       | Description                                  |
|--------------|----------------------------------------------|
| `song_id`    | Unique ID of the song                        |
| `title`      | Title of the song                            |
| `artist`     | Name of the artist                           |
| `genre`      | Genre of the song (e.g., Pop, Jazz)          |
| `mood`       | Mood of the song (e.g., Happy, Sad)          |

## 🛠️ Tasks Performed

### 1. **User’s Favorite Genre**
- Computed the most-listened genre for each user.
- 📁 Output: `output/user_favorite_genres/`

### 2. **Average Listen Time per Song**
- Calculated average duration (in seconds) for each song.
- 📁 Output: `output/avg_listen_time_per_song/`

### 3. **Top 10 Most Played Songs This Week**
- Extracted songs most played in the current week (week of 2025-03-23).
- 📁 Output: `output/top_songs_this_week/`

### 4. **"Happy" Song Recommendations for "Sad" Listeners**
- Identified users who primarily listen to “Sad” songs and recommended “Happy” songs they haven’t heard.
- 📁 Output: `output/happy_recommendations/`

### 5. **Genre Loyalty Score**
- Calculated the proportion of plays belonging to the favorite genre.
- Filtered users with loyalty score > 0.8.
- 📁 Output: `output/genre_loyalty_scores/`

### 6. **Night Owl Users**
- Extracted users who frequently listen to music between 12 AM and 5 AM.
- 📁 Output: `output/night_owl_users/`

### 7. **Enriched Logs (Optional Bonus)**
- Joined listening logs with song metadata for enriched analysis.
- 📁 Output: `output/enriched_logs/`

## ⚙️ Running the Scripts

### 🚀 Run via Spark Submit:
```bash
spark-submit spark_analysis.py
