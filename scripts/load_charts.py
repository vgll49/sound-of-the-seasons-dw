# scripts/load_charts.py
import sys
import os

import pandas as pd
from sqlalchemy.orm import Session
from db.models import DimTrack, DimTime, FactTrackChart
from db.database import SessionLocal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_tracks_and_facts():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(project_root, "data", "processed", "spotify_charts_de.csv")
    
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date'])
    
    db: Session = SessionLocal()
    
    try:
        # 1. Load unique tracks into DimTrack
        logger.info("Loading tracks into DimTrack...")
        unique_tracks = df.drop_duplicates(subset=['track_id'])
        unique_tracks = unique_tracks.dropna(subset=['name'])

        
        track_records = []
        for _, row in unique_tracks.iterrows():
            # artists: z.B. ['Haftbefehl'] → 'Haftbefehl'
            if isinstance(row['artists'], str) and row['artists'].startswith("["):
                import ast
                artists_list = ast.literal_eval(row['artists'])
                artists_str = ", ".join(artists_list)
            else:
                artists_str = row['artists']

            if isinstance(row.get('artist_genres'), str) and row['artist_genres'].startswith("["):
                genres_list = ast.literal_eval(row['artist_genres'])
                genres_str = ", ".join(genres_list)
            else:
                genres_str = row.get('artist_genres', None)

            track = DimTrack(
                track_id=row['track_id'],
                track_name=row['name'],
                artist_names=artists_str,
                genre=genres_str,
                duration_ms=row.get('duration', None),
                explicit_flag=row.get('explicit', False)
            )
            track_records.append(track)
        
        db.bulk_save_objects(track_records)
        db.commit()
        logger.info(f"✓ Inserted {len(track_records)} tracks")
        
        # 2. Create date lookup
        logger.info("Creating date lookup...")
        date_lookup = {}
        dates = db.query(DimTime.date_id, DimTime.date).all()
        for d in dates:
            date_lookup[d.date] = d.date_id
        
        # 3. Load facts into FactTrackChart
        logger.info("Loading chart facts...")
        fact_records = []
        batch_size = 1000
        
        for idx, row in df.iterrows():
            date_id = date_lookup.get(row['date'].date())
            if not date_id:
                logger.warning(f"Date {row['date']} not found in DimTime")
                continue
            
            fact = FactTrackChart(
                track_id=row['track_id'],
                date_id=date_id,
                stream_count=row['streams'],
                chart_position=row['position'],
                # weather_id and holiday_id werden später verknüpft
                weather_id=None,
                holiday_id=None
            )
            fact_records.append(fact)
            
            if len(fact_records) >= batch_size:
                db.bulk_save_objects(fact_records)
                db.commit()
                logger.info(f"Inserted {len(fact_records)} facts (total: {idx+1})")
                fact_records = []
        
        # Insert remaining
        if fact_records:
            db.bulk_save_objects(fact_records)
            db.commit()
        
        total_facts = db.query(FactTrackChart).count()
        logger.info(f"✓ Total facts in database: {total_facts}")
        
    except Exception as e:
        db.rollback()
        logger.error(f"✗ Error: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    load_tracks_and_facts()