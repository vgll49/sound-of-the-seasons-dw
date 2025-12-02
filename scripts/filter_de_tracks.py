import pandas as pd
import os

# setup root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# paths
input_csv = os.path.join(project_root, "data", "raw", "charts.csv")
output_csv = os.path.join(project_root, "data", "processed", "spotify_charts_de.csv")

# read csv
df = pd.read_csv(input_csv)

# filter de
df_de = df[df['country'] == 'de'].copy()

columns_to_keep = [
    'date', 'country', 'position', 'streams', 'track_id', 
    'artists', 'artist_genres', 'duration', 'explicit', 'name'
]
df_de = df_de[columns_to_keep]

df_de['date'] = pd.to_datetime(df_de['date'], errors='coerce')
df_de = df_de.dropna(subset=['date'])

# save new file
df_de.to_csv(output_csv, index=False)

print(f"Filtered CSV saved to {output_csv}")
