# scripts/load_charts.py
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
    
    # Charts CSV (Global + DE)
    charts_csv = os.path.join(project_root, "data", "processed", "spotify_charts_global_de.csv")
    
    # Audio Features CSV
    tracks_csv = os.path.join(project_root, "data", "raw", "tracks.csv")
    
    # Load Charts
    logger.info("Loading charts data...")
    charts_df = pd.read_csv(charts_csv)
    charts_df['date'] = pd.to_datetime(charts_df['date'])
    
    logger.info(f"  Charts: {len(charts_df):,} rows")
    logger.info(f"  Countries: {charts_df['country'].unique()}")
    logger.info(f"  Unique tracks: {charts_df['track_id'].nunique():,}")
    
    # Load Audio Features
    audio_features_dict = {}
    if os.path.exists(tracks_csv):
        logger.info("Loading audio features...")
        audio_features_df = pd.read_csv(tracks_csv)
        
        if 'id' in audio_features_df.columns and 'track_id' not in audio_features_df.columns:
            audio_features_df.rename(columns={'id': 'track_id'}, inplace=True)
        
        audio_features_df = audio_features_df.drop_duplicates(subset=['track_id'], keep='first')
        
        # Create lookup dict
        for _, row in audio_features_df.iterrows():
            audio_features_dict[row['track_id']] = {
                'danceability': row.get('danceability'),
                'energy': row.get('energy'),
                'valence': row.get('valence'),
                'tempo': row.get('tempo'),
                'loudness': row.get('loudness'),
                'speechiness': row.get('speechiness'),
                'acousticness': row.get('acousticness'),
                'instrumentalness': row.get('instrumentalness'),
                'liveness': row.get('liveness'),
                'key': row.get('key'),
                'mode': row.get('mode'),
                'time_signature': row.get('time_signature'),
                'popularity': row.get('popularity'),
                'release_date': row.get('release_date')
            }
        
        logger.info(f"  Audio features: {len(audio_features_dict):,} unique tracks")
    else:
        logger.warning(f"  Audio features file not found: {tracks_csv}")
    
    db: Session = SessionLocal()
    
    try:
        # 1. Load unique tracks into DimTrack
        logger.info("\nPreparing tracks...")
        unique_tracks = charts_df[['track_id', 'name', 'artists', 'artist_genres', 'duration', 'explicit']].copy()
        unique_tracks = unique_tracks.drop_duplicates(subset=['track_id'])
        unique_tracks = unique_tracks.dropna(subset=['name'])
        
        logger.info(f"  Unique tracks: {len(unique_tracks):,}")
        
        logger.info("Creating track records...")
        track_records = []
        tracks_with_features = 0
        
        def safe_float(val):
            return float(val) if pd.notna(val) else None
        
        def safe_int(val):
            return int(val) if pd.notna(val) else None
        
        for track_id, name, artists, genre, duration, explicit in zip(
            unique_tracks['track_id'],
            unique_tracks['name'],
            unique_tracks['artists'],
            unique_tracks.get('artist_genres', [None]*len(unique_tracks)),
            unique_tracks.get('duration', [None]*len(unique_tracks)),
            unique_tracks.get('explicit', [False]*len(unique_tracks))
        ):
            features = audio_features_dict.get(track_id, {})
            
            track = DimTrack(
                track_id=track_id,
                track_name=name,
                artist_names=artists,
                genre=genre,
                duration_ms=duration,
                explicit_flag=explicit,
                # Meta
                popularity=safe_int(features.get('popularity')),
                release_date=features.get('release_date'),
                # Core Audio Features
                danceability=safe_float(features.get('danceability')),
                energy=safe_float(features.get('energy')),
                valence=safe_float(features.get('valence')),
                tempo=safe_float(features.get('tempo')),
                # Extended Audio Features
                loudness=safe_float(features.get('loudness')),
                speechiness=safe_float(features.get('speechiness')),
                acousticness=safe_float(features.get('acousticness')),
                instrumentalness=safe_float(features.get('instrumentalness')),
                liveness=safe_float(features.get('liveness')),
                key=safe_int(features.get('key')),
                mode=safe_int(features.get('mode')),
                time_signature=safe_int(features.get('time_signature'))
            )
            
            if track.danceability is not None:
                tracks_with_features += 1
            
            track_records.append(track)
        
        # Bulk insert tracks
        logger.info(f"Inserting {len(track_records):,} tracks...")
        db.bulk_save_objects(track_records)
        db.commit()
        
        total_tracks = len(track_records)
        coverage = (tracks_with_features / total_tracks * 100) if total_tracks > 0 else 0
        
        logger.info(f"✓ Inserted {total_tracks:,} tracks")
        logger.info(f"  With audio features:    {tracks_with_features:,} ({coverage:.1f}%)")
        logger.info(f"  Without audio features: {total_tracks - tracks_with_features:,}")
        
        # 2. Create date lookup
        logger.info("\nCreating date lookup...")
        date_lookup = {d.date: d.date_id for d in db.query(DimTime.date_id, DimTime.date).all()}
        logger.info(f"  Loaded {len(date_lookup):,} dates")
        
        # 3. Load facts into FactTrackChart
        logger.info("Preparing chart facts...")
        
        charts_df['date_only'] = charts_df['date'].dt.date
        charts_df['date_id'] = charts_df['date_only'].map(date_lookup)
        
        valid_facts = charts_df[charts_df['date_id'].notna()].copy()
        skipped = len(charts_df) - len(valid_facts)
        
        if skipped > 0:
            logger.warning(f"  Skipping {skipped:,} rows without matching dates")
        
        logger.info(f"  Creating {len(valid_facts):,} fact records...")
        
        # Create fact records with country
        fact_records = [
            FactTrackChart(
                track_id=row['track_id'],
                date_id=int(row['date_id']),
                country=row['country'],  # NEU: Country speichern!
                stream_count=row['streams'],
                chart_position=row['position'],
                weather_id=None,
                holiday_id=None
            )
            for _, row in valid_facts.iterrows()
        ]
        
        # Bulk insert in batches
        logger.info("Inserting facts...")
        batch_size = 5000
        
        for i in range(0, len(fact_records), batch_size):
            batch = fact_records[i:i+batch_size]
            db.bulk_save_objects(batch)
            db.flush()
            
            if (i + batch_size) % 10000 == 0:
                logger.info(f"  Progress: {i+batch_size:,}/{len(fact_records):,}")
        
        db.commit()
        
        total_facts = db.query(FactTrackChart).count()
        logger.info(f"✓ Total facts in database: {total_facts:,}")
        
        # Country breakdown
        logger.info("\nBreakdown by country:")
        for country in ['de', 'global']:
            count = db.query(FactTrackChart).filter(FactTrackChart.country == country).count()
            logger.info(f"  {country:8s}: {count:,}")
        
        # Final summary
        logger.info(f"\n{'='*60}")
        logger.info("SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Tracks loaded:          {total_tracks:,}")
        logger.info(f"Audio feature coverage: {coverage:.1f}%")
        logger.info(f"Facts loaded:           {total_facts:,}")
        logger.info(f"  - DE:                 {db.query(FactTrackChart).filter(FactTrackChart.country == 'de').count():,}")
        logger.info(f"  - Global:             {db.query(FactTrackChart).filter(FactTrackChart.country == 'global').count():,}")
        logger.info(f"Skipped (no date):      {skipped:,}")
        logger.info(f"{'='*60}")
        
    except Exception as e:
        db.rollback()
        logger.error(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    load_tracks_and_facts()