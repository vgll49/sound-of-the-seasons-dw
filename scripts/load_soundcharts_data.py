import os
import pandas as pd
from services.data_loader import DataLoader
from db.database import SessionLocal
from db.models import DimTrack, FactTrackChart
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
    
    loader = DataLoader(batch_size=BATCH_SIZE_FACTS)
    
    # 1. Load Tracks
    logger.info("\nLoading tracks into DimTrack...")
    loader.load_tracks_bulk(features_df)
    
    # 2. Load Facts
    logger.info("\nLoading chart facts...")
    loader.load_facts_bulk(charts_df)
    
    # 3. Summary
    db = SessionLocal()
    
    total_tracks = db.query(DimTrack).count()
    total_facts = db.query(FactTrackChart).count()
    with_features = db.query(DimTrack).filter(DimTrack.danceability.isnot(None)).count()
    with_weather = db.query(FactTrackChart).filter(FactTrackChart.weather_id.isnot(None)).count()
    
    db.close()
    
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

if __name__ == "__main__":
    load_soundcharts_data()