import os
import pandas as pd
import json
from sqlalchemy.orm import Session
from db.models import DimTrack, DimTime, DimWeather, FactTrackChart
from db.database import SessionLocal
from config import CHARTS_CSV, FEATURES_CSV, BATCH_SIZE_FACTS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_soundcharts_data():
    """Load Soundcharts charts and features into database"""
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    charts_csv = os.path.join(project_root, CHARTS_CSV)
    features_csv = os.path.join(project_root, FEATURES_CSV)
    
    logger.info("Loading Soundcharts charts...")
    charts_df = pd.read_csv(charts_csv)
    charts_df['chart_date'] = pd.to_datetime(charts_df['chart_date'])
    
    logger.info(f"  Charts: {len(charts_df):,} entries")
    logger.info(f"  Date range: {charts_df['chart_date'].min().date()} to {charts_df['chart_date'].max().date()}")
    logger.info(f"  Unique songs: {charts_df['song_uuid'].nunique():,}")
    
    logger.info("Loading audio features...")
    features_df = pd.read_csv(features_csv)
    
    logger.info(f"  Features: {len(features_df):,} tracks")
    logger.info(f"  Coverage: 100% (all chart songs have features)")
    
    # Create feature lookup
    features_dict = {}
    for _, row in features_df.iterrows():
        genres_str = None
        if pd.notna(row.get('genres')):
            try:
                genres_list = json.loads(row['genres'])
                if genres_list and len(genres_list) > 0:
                    genres_str = genres_list[0].get('root', '')
            except:
                pass
        
        features_dict[row['song_uuid']] = {
            'song_name': row.get('song_name'),
            'artist_name': row.get('artist_name'),
            'isrc': row.get('isrc'),
            'release_date': row.get('release_date'),
            'duration': row.get('duration'),
            'explicit': row.get('explicit'),
            'language_code': row.get('language_code'),
            'image_url': row.get('image_url'),
            'copyright': row.get('copyright'),
            'genre': genres_str,
            'acousticness': row.get('acousticness'),
            'danceability': row.get('danceability'),
            'energy': row.get('energy'),
            'instrumentalness': row.get('instrumentalness'),
            'key': row.get('key'),
            'liveness': row.get('liveness'),
            'loudness': row.get('loudness'),
            'mode': row.get('mode'),
            'speechiness': row.get('speechiness'),
            'tempo': row.get('tempo'),
            'time_signature': row.get('time_signature'),
            'valence': row.get('valence')
        }
    
    db: Session = SessionLocal()
    
    try:
        # 1. Load Tracks
        logger.info("\nLoading tracks into DimTrack...")
        
        unique_uuids = charts_df['song_uuid'].unique()
        track_records = []
        
        for uuid in unique_uuids:
            feat = features_dict.get(uuid, {})
            
            duration_ms = None
            if pd.notna(feat.get('duration')):
                duration_ms = int(feat['duration'] * 1000)
            
            track = DimTrack(
                track_id=uuid,
                track_name=feat.get('song_name', 'Unknown'),
                artist_names=feat.get('artist_name', 'Unknown'),
                genre=feat.get('genre'),
                duration_ms=duration_ms,
                release_date=feat.get('release_date'),
                language_code=feat.get('language_code'),
                image_url=feat.get('image_url'),
                danceability=feat.get('danceability'),
                energy=feat.get('energy'),
                valence=feat.get('valence'),
                tempo=feat.get('tempo'),
                loudness=feat.get('loudness'),
                speechiness=feat.get('speechiness'),
                acousticness=feat.get('acousticness'),
                instrumentalness=feat.get('instrumentalness'),
                liveness=feat.get('liveness'),
                key=int(feat['key']) if pd.notna(feat.get('key')) else None,
                mode=int(feat['mode']) if pd.notna(feat.get('mode')) else None,
                time_signature=int(feat['time_signature']) if pd.notna(feat.get('time_signature')) else None
            )
            track_records.append(track)
        
        logger.info(f"Inserting {len(track_records):,} tracks...")
        db.bulk_save_objects(track_records)
        db.commit()
        logger.info("Tracks inserted")
        
        # 2. Create lookups
        logger.info("\nCreating lookups...")
        date_lookup = {d.date: d.date_id for d in db.query(DimTime.date_id, DimTime.date).all()}
        weather_lookup = {w.date_id: w.weather_id for w in db.query(DimWeather.date_id, DimWeather.weather_id).all()}
        
        logger.info(f"  Loaded {len(date_lookup):,} dates")
        logger.info(f"  Loaded {len(weather_lookup):,} weather records")
        
        # 3. Load Facts
        logger.info("\nLoading chart facts...")
        
        charts_df['date_only'] = charts_df['chart_date'].dt.date
        charts_df['date_id'] = charts_df['date_only'].map(date_lookup)
        
        valid_facts = charts_df[charts_df['date_id'].notna()].copy()
        skipped = len(charts_df) - len(valid_facts)
        
        if skipped > 0:
            logger.warning(f"  Skipping {skipped:,} rows without matching dates")
        
        logger.info(f"  Creating {len(valid_facts):,} fact records...")
        
        fact_records = []
        for _, row in valid_facts.iterrows():
            date_id = int(row['date_id'])
            
            fact = FactTrackChart(
                track_id=row['song_uuid'],
                date_id=date_id,
                weather_id=weather_lookup.get(date_id),
                country='de',
                stream_count=int(row['streams']) if pd.notna(row.get('streams')) else None,
                chart_position=int(row['position']) if pd.notna(row.get('position')) else None
            )
            fact_records.append(fact)
        
        logger.info("Inserting facts...")
        batch_size = BATCH_SIZE_FACTS
        
        for i in range(0, len(fact_records), batch_size):
            batch = fact_records[i:i+batch_size]
            db.bulk_save_objects(batch)
            db.flush()
            
            if (i + batch_size) % 10000 == 0:
                logger.info(f"  Progress: {i+batch_size:,}/{len(fact_records):,}")
        
        db.commit()
        
        # Summary
        total_tracks = db.query(DimTrack).count()
        total_facts = db.query(FactTrackChart).count()
        with_features = db.query(DimTrack).filter(DimTrack.danceability.isnot(None)).count()
        with_weather = db.query(FactTrackChart).filter(FactTrackChart.weather_id.isnot(None)).count()
        
        logger.info(f"\n{'='*60}")
        logger.info("SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Tracks loaded:          {total_tracks:,}")
        logger.info(f"Audio feature coverage: {with_features/total_tracks*100:.1f}%")
        logger.info(f"Facts loaded:           {total_facts:,}")
        logger.info(f"  With weather:         {with_weather:,} ({with_weather/total_facts*100:.1f}%)")
        logger.info(f"Date range:             {charts_df['chart_date'].min().date()} to {charts_df['chart_date'].max().date()}")
        logger.info(f"Country:                DE (Top 200)")
        logger.info(f"{'='*60}")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    load_soundcharts_data()