import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Set random seed for reproducibility
random.seed(42)

# Generate songs_metadata.csv
def generate_songs_metadata(num_songs=100):
    genres = ["Pop", "Rock", "Jazz", "Hip-Hop", "Classical"]
    moods = ["Happy", "Sad", "Energetic", "Chill"]
    
    songs = []
    for i in range(1, num_songs + 1):
        song = {
            "song_id": f"S{i:04d}",
            "title": fake.sentence(nb_words=3).replace('.', ''),
            "artist": fake.name(),
            "genre": random.choice(genres),
            "mood": random.choice(moods)
        }
        songs.append(song)
        
    df_songs = pd.DataFrame(songs)
    df_songs.to_csv("songs_metadata.csv", index=False)
    print("Generated songs_metadata.csv")

# Generate listening_logs.csv
def generate_listening_logs(num_users=50, num_logs=1000, song_ids=None):
    logs = []
    for _ in range(num_logs):
        user_id = f"U{random.randint(1, num_users):03d}"
        song_id = random.choice(song_ids)
        timestamp = fake.date_time_between(start_date='-30d', end_date='now')
        duration = random.randint(30, 300)  # 30 seconds to 5 minutes
        
        log = {
            "user_id": user_id,
            "song_id": song_id,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_sec": duration
        }
        logs.append(log)
    
    df_logs = pd.DataFrame(logs)
    df_logs.to_csv("listening_logs.csv", index=False)
    print("Generated listening_logs.csv")

# Generate datasets
generate_songs_metadata(num_songs=100)
song_df = pd.read_csv("songs_metadata.csv")
generate_listening_logs(num_users=50, num_logs=2000, song_ids=song_df["song_id"].tolist())
