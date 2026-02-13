import pandas as pd
import os

def prepare_charts():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Paths
    input_csv = os.path.join(project_root, "data", "raw", "charts.csv")
    output_csv = os.path.join(project_root, "data", "processed", "spotify_charts_global_de.csv")
    
    # Read CSV
    print("Loading charts data...")
    df = pd.read_csv(input_csv)
    
    print(f"  Total rows: {len(df):,}")
    print(f"  Countries in dataset: {sorted(df['country'].unique())}")
    
    # Filter: Global UND Deutschland
    df_filtered = df[df['country'].isin(['global', 'de'])].copy()
    
    print(f"\nFiltered to 'global' and 'de':")
    print(f"  Global rows: {len(df_filtered[df_filtered['country'] == 'Global']):,}")
    print(f"  DE rows:     {len(df_filtered[df_filtered['country'] == 'de']):,}")
    print(f"  Total:       {len(df_filtered):,}")
    
    # Keep columns
    columns_to_keep = [
        'date', 'country', 'position', 'streams', 'track_id', 
        'artists', 'artist_genres', 'duration', 'explicit', 'name'
    ]
    
    df_filtered = df_filtered[columns_to_keep]
    
    # Parse dates
    df_filtered['date'] = pd.to_datetime(df_filtered['date'], errors='coerce')
    df_filtered = df_filtered.dropna(subset=['date'])
    
    # Save
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df_filtered.to_csv(output_csv, index=False)
    
    print(f"\n✓ Filtered CSV saved to: {output_csv}")
    print(f"\nStats:")
    print(f"  Date range: {df_filtered['date'].min()} to {df_filtered['date'].max()}")
    print(f"  Unique tracks: {df_filtered['track_id'].nunique():,}")
    print(f"\nBreakdown by country:")
    print(df_filtered.groupby('country').size())

if __name__ == "__main__":
    prepare_charts()