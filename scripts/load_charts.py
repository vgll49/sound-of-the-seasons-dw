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
    
    # Charts CSV
    charts_csv = os.path.join(project_root, "data", "processed", "spotify_charts_de.csv")
    
    # Audio Features CSV
    tracks_csv = os.path.join(project_root, "data", "raw", "tracks.csv")
    
    # Load Charts
    logger.info("Loading charts data...")
    charts_df = pd.read_csv(charts_csv)
    charts_df['date'] = pd.to_datetime(charts_df['date'])
    
    logger.info(f"  Charts: {len(charts_df):,} rows, {charts_df['track_id'].nunique():,} unique tracks")
    
    # Load Audio Features (optional)
    audio_features_df = None
    if os.path.exists(tracks_csv):
        logger.info("Loading audio features...")
        audio_features_df = pd.read_csv(tracks_csv)
        
        # Rename 'id' to 'track_id' if needed
        if 'id' in audio_features_df.columns and 'track_id' not in audio_features_df.columns:
            audio_features_df.rename(columns={'id': 'track_id'}, inplace=True)
        
        # Remove duplicates
        audio_features_df = audio_features_df.drop_duplicates(subset=['track_id'], keep='first')
        
        logger.info(f"  Audio features: {len(audio_features_df):,} unique tracks")
    else:
        logger.warning(f"  Audio features file not found: {tracks_csv}")
        logger.warning("  Continuing without audio features...")
    
    db: Session = SessionLocal()
    
    try:
        # 1. Load unique tracks into DimTrack
        logger.info("\nLoading tracks into DimTrack...")
        unique_tracks = charts_df.drop_duplicates(subset=['track_id'])
        unique_tracks = unique_tracks.dropna(subset=['name'])

        track_records = []
        tracks_with_features = 0
        
        for _, row in unique_tracks.iterrows():
            # Basic track info
            track = DimTrack(
                track_id=row['track_id'],
                track_name=row['name'],
                artist_names=row['artists'],
                genre=row.get('artist_genres', None),
                duration_ms=row.get('duration', None),
                explicit_flag=row.get('explicit', False)
            )
            
            # Try to add audio features
            if audio_features_df is not None:
                feature_row = audio_features_df[audio_features_df['track_id'] == row['track_id']]
                
                if not feature_row.empty:
                    feat = feature_row.iloc[0]
                    
                    # Add all available audio features
                    track.danceability = float(feat['danceability']) if pd.notna(feat.get('danceability')) else None
                    track.energy = float(feat['energy']) if pd.notna(feat.get('energy')) else None
                    track.valence = float(feat['valence']) if pd.notna(feat.get('valence')) else None
                    track.tempo = float(feat['tempo']) if pd.notna(feat.get('tempo')) else None
                    
                    tracks_with_features += 1
            
            track_records.append(track)
        
        # Save to DB
        db.bulk_save_objects(track_records)
        db.commit()
        
        total_tracks = len(track_records)
        coverage = (tracks_with_features / total_tracks * 100) if total_tracks > 0 else 0
        
        logger.info(f"✓ Inserted {total_tracks:,} tracks")
        logger.info(f"  With audio features:    {tracks_with_features:,} ({coverage:.1f}%)")
        logger.info(f"  Without audio features: {total_tracks - tracks_with_features:,}")
        
        # Coverage assessment
        if coverage >= 70:
            logger.info(f"  🎉 Excellent coverage!")
        elif coverage >= 50:
            logger.info(f"  👍 Good coverage")
        elif coverage >= 30:
            logger.warning(f"  ⚠️  Moderate coverage - consider filtering to tracks with features")
        else:
            logger.warning(f"  ❌ Low coverage - audio features might not match your charts")
        
        # 2. Create date lookup
        logger.info("\nCreating date lookup...")
        date_lookup = {}
        dates = db.query(DimTime.date_id, DimTime.date).all()
        for d in dates:
            date_lookup[d.date] = d.date_id
        
        # 3. Load facts into FactTrackChart
        logger.info("Loading chart facts...")
        fact_records = []
        batch_size = 1000
        skipped = 0
        
        for idx, row in charts_df.iterrows():
            date_id = date_lookup.get(row['date'].date())
            if not date_id:
                #logger.warning(f"Date {row['date']} not found in DimTime")
                skipped += 1
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
                logger.info(f"  Inserted {idx+1:,}/{len(charts_df):,} facts...")
                fact_records = []
        
        # Insert remaining
        if fact_records:
            db.bulk_save_objects(fact_records)
            db.commit()
        
        total_facts = db.query(FactTrackChart).count()
        logger.info(f"✓ Total facts in database: {total_facts:,}")
        
        if skipped > 0:
            logger.warning(f"  Skipped {skipped} rows (date not in DimTime)")
        
        # Final summary
        logger.info(f"\n{'='*60}")
        logger.info("SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Tracks loaded:        {total_tracks:,}")
        logger.info(f"Audio feature coverage: {coverage:.1f}%")
        logger.info(f"Facts loaded:         {total_facts:,}")
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
