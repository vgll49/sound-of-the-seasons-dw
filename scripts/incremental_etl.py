import asyncio
import aiohttp
import os
import logging
from datetime import datetime, timedelta, date

from scripts.populate_dim_time import populate_dim_time
from services.soundcharts_service import SoundchartsService
from services.data_loader import DataLoader
from services.weather_service import WeatherService
from db.database import SessionLocal
from db.models import DimTime, DimWeather, DimTrack, FactTrackChart
from config import BATCH_SIZE_WEATHER

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_credentials():
    """Get API credentials from environment"""
    app_id = os.getenv('SOUNDCHARTS_APP_ID')
    api_key = os.getenv('SOUNDCHARTS_API_KEY')
    
    if not app_id or not api_key:
        raise ValueError("Missing SOUNDCHARTS_APP_ID or SOUNDCHARTS_API_KEY")
    
    return app_id, api_key


def extend_dim_time_to_today():
    """Extend DimTime from latest date to today if needed"""
    db = SessionLocal()
    
    try:
        latest = db.query(DimTime.date).order_by(DimTime.date.desc()).first()
        
        if not latest:
            logger.error("DimTime is empty! Run full ETL first.")
            return
        
        latest_date = latest.date
        today = date.today()
        
        logger.info(f"DimTime latest: {latest_date}, Today: {today}")
        
        if latest_date >= today:
            logger.info("DimTime is up to date")
            return
        
        logger.info("Extending DimTime to today...")
        
    finally:
        db.close()
    
    populate_dim_time(
        start_date=latest_date + timedelta(days=1),
        end_date=today
    )

def get_missing_chart_dates():
    """Find new Sundays after the latest chart in DB"""
    db = SessionLocal()
    
    latest_fact_date = db.query(DimTime.date).join(
        FactTrackChart, DimTime.date_id == FactTrackChart.date_id
    ).order_by(DimTime.date.desc()).first()
    
    if not latest_fact_date:
        logger.warning("No facts in DB - run full ETL first!")
        db.close()
        return []
    
    latest_date = latest_fact_date.date
    logger.info(f"Latest date with facts: {latest_date}")
    
    all_dates = db.query(DimTime.date).filter(
        DimTime.date > latest_date
    ).all()
    
    sundays = sorted([d.date for d in all_dates if d.date.weekday() == 6])
    
    db.close()
    
    logger.info(f"New Sundays to fetch: {len(sundays)}")
    if sundays:
        logger.info(f"  Range: {sundays[0]} to {sundays[-1]}")
    
    return sundays


def get_missing_features():
    """Find tracks without audio features"""
    db = SessionLocal()
    
    missing = [
        t.track_id for t in db.query(DimTrack.track_id).filter(
            DimTrack.danceability.is_(None)
        ).all()
    ]
    
    db.close()
    
    logger.info(f"Tracks missing features: {len(missing)}")
    return missing[:200]


def get_missing_weather_dates():
    """Find new dates after the latest weather in DB"""
    db = SessionLocal()
    
    latest = db.query(DimTime.date).join(
        DimWeather, DimTime.date_id == DimWeather.date_id
    ).order_by(DimTime.date.desc()).first()
    
    if not latest:
        logger.warning("No weather in DB - run full ETL first!")
        db.close()
        return []
    
    latest_date = latest.date
    logger.info(f"Latest date with weather: {latest_date}")
    
    missing = db.query(DimTime.date).filter(
        DimTime.date > latest_date
    ).all()
    
    db.close()
    
    result = sorted([d.date for d in missing])
    logger.info(f"New dates for weather: {len(result)}")
    
    return result

async def fetch_and_load_charts(app_id, api_key, missing_dates):
    """Fetch charts for missing Sundays and return NEW track IDs only"""
    if not missing_dates:
        logger.info("No charts to fetch")
        return []
    
    logger.info(f"Fetching {len(missing_dates)} charts...")
    
    loader = DataLoader()
    all_new_track_ids = []  # ← Only NEW tracks
    total = 0
    
    async with aiohttp.ClientSession() as session:
        service = SoundchartsService(session, app_id, api_key)
        
        for date_obj in sorted(missing_dates):
            api_date_str = f"{date_obj.isoformat()}T12:00:00+00:00"
            
            logger.info(f"  {date_obj}...")
            items = await service.fetch_chart_for_date('top-songs-22', api_date_str, top_n=200)
            
            if items:
                inserted, new_ids = loader.load_charts(items, date_obj, create_tracks=True)  # ← Get new IDs!
                
                all_new_track_ids.extend(new_ids)  # ← Only new ones
                total += inserted
                logger.info(f"    Inserted {inserted} facts, {len(new_ids)} new tracks")
            else:
                logger.warning(f"    No data")
            
            await asyncio.sleep(0.5)
    
    logger.info(f"Total: {total} facts, {len(all_new_track_ids)} NEW tracks need features")
    return all_new_track_ids


async def fetch_and_load_features(app_id, api_key, track_ids):
    """Fetch audio features for track IDs"""
    if not track_ids:
        logger.info("No features to fetch")
        return
    
    logger.info(f"Fetching {len(track_ids)} features...")
    
    loader = DataLoader()
    
    async with aiohttp.ClientSession() as session:
        service = SoundchartsService(session, app_id, api_key)
        df = await service.fetch_audio_features(track_ids)
    
    if len(df) > 0:
        loader.update_track_features(df)


async def fetch_and_load_weather(missing_dates):
    """Fetch weather for missing dates"""
    if not missing_dates:
        logger.info("No weather to fetch")
        return
    
    logger.info(f"Fetching weather for {len(missing_dates)} dates...")
    
    # Group into ranges
    ranges = []
    start = missing_dates[0]
    end = missing_dates[0]
    
    for d in missing_dates[1:]:
        if (d - end).days == 1:
            end = d
        else:
            ranges.append((start, end))
            start = d
            end = d
    ranges.append((start, end))
    
    logger.info(f"Consolidated into {len(ranges)} ranges")
    
    loader = DataLoader(batch_size=BATCH_SIZE_WEATHER)
    
    async with aiohttp.ClientSession() as session:
        for start, end in ranges:
            logger.info(f"  {start} to {end}")
            
            service = WeatherService(
                session=session,
                start_date=start.isoformat(),
                end_date=end.isoformat()
            )
            
            await loader.load_weather(service)
            await asyncio.sleep(5)

async def main():
    """Main incremental ETL"""
    logger.info("="*70)
    logger.info("INCREMENTAL ETL")
    logger.info("="*70)
    logger.info(f"Timestamp: {datetime.now()}")
    
    # Credentials
    app_id, api_key = get_credentials()
    
    # Extend DimTime
    logger.info("\n--- Extending DimTime ---")
    extend_dim_time_to_today()
    
    # Identify missing
    logger.info("\n--- Checking for new data ---")
    missing_weather = get_missing_weather_dates()
    missing_charts = get_missing_chart_dates()
    missing_features_old = get_missing_features()
    
    if not any([missing_weather, missing_charts, missing_features_old]):
        logger.info("\nDatabase is up to date!")
        return
    
    # Fetch
    logger.info("\n--- Fetching new data ---")
    
    await fetch_and_load_weather(missing_weather)
    new_track_ids = await fetch_and_load_charts(app_id, api_key, missing_charts)
    
    if new_track_ids:
        logger.info(f"\n--- Features for {len(new_track_ids)} new tracks ---")
        await fetch_and_load_features(app_id, api_key, new_track_ids)
    
    if missing_features_old:
        logger.info(f"\n--- Features for {len(missing_features_old)} old tracks ---")
        await fetch_and_load_features(app_id, api_key, missing_features_old)
    
    logger.info("\n" + "="*70)
    logger.info("INCREMENTAL ETL COMPLETE")
    logger.info("="*70)


if __name__ == "__main__":
    asyncio.run(main())