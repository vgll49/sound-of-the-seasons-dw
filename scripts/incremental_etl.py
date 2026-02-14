"""
Incremental ETL - Fetch only missing/new data
"""
import asyncio
import aiohttp
import os
import logging
from datetime import datetime

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

class IncrementalETL:
    """Identifies missing data from DB"""
    
    def __init__(self):
        self.app_id = os.getenv('SOUNDCHARTS_APP_ID')
        self.api_key = os.getenv('SOUNDCHARTS_API_KEY')
        
        if not self.app_id or not self.api_key:
            raise ValueError("Missing SOUNDCHARTS_APP_ID or SOUNDCHARTS_API_KEY")
    
    def get_missing_chart_dates(self):
        """Find chart dates missing from FactTrackChart"""
        db = SessionLocal()
        
        all_dates = {d.date for d in db.query(DimTime.date).all()}
        sundays = sorted([d for d in all_dates if d.weekday() == 6])
        
        fact_dates = {d.date for d in db.query(DimTime.date, DimTime.date_id).join(
            FactTrackChart, DimTime.date_id == FactTrackChart.date_id
        ).distinct().all()}
        
        db.close()
        
        missing = sorted(list(set(sundays) - fact_dates))
        logger.info(f"Missing chart dates: {len(missing)}")
        
        return missing
    
    def get_missing_features(self):
        """Find tracks without audio features"""
        db = SessionLocal()
        
        missing_uuids = [
            t.track_id for t in db.query(DimTrack.track_id).filter(
                DimTrack.danceability.is_(None)
            ).all()
        ]
        
        db.close()
        
        logger.info(f"Missing features: {len(missing_uuids)}")
        return missing_uuids[:200]  # Limit per run
    
    def get_missing_weather_dates(self):
        """Find dates without weather"""
        db = SessionLocal()
        
        all_dates = {d.date for d in db.query(DimTime.date).all()}
        
        weather_date_ids = {w.date_id for w in db.query(DimWeather.date_id).all()}
        dates_with_weather = {d.date for d in db.query(DimTime.date, DimTime.date_id).filter(
            DimTime.date_id.in_(weather_date_ids)
        ).all()}
        
        db.close()
        
        missing = sorted(list(all_dates - dates_with_weather))
        logger.info(f"Missing weather: {len(missing)}")
        
        return missing


async def fetch_and_load_charts(etl: IncrementalETL, missing_dates):
    """Fetch charts and load using DataLoader"""
    if not missing_dates:
        logger.info("No charts to fetch")
        return
    
    logger.info(f"Fetching {len(missing_dates)} charts...")
    
    loader = DataLoader()
    
    async with aiohttp.ClientSession() as session:
        service = SoundchartsService(session, etl.app_id, etl.api_key)
        
        # Get available API dates
        all_api_dates = await service.fetch_available_chart_dates('top-songs-22')
        
        # Map to missing dates
        date_map = {}
        for api_date_str in all_api_dates:
            dt = datetime.fromisoformat(api_date_str.replace('+00:00', ''))
            date_obj = dt.date()
            if date_obj in missing_dates:
                date_map[date_obj] = api_date_str
        
        # Fetch and load each
        total_inserted = 0
        for date_obj, api_str in date_map.items():
            logger.info(f"  Fetching {date_obj}...")
            items = await service.fetch_chart_for_date('top-songs-22', api_str, top_n=200)
            
            if items:
                inserted = loader.load_charts(items, date_obj, create_tracks=True)
                total_inserted += inserted
            
            await asyncio.sleep(0.5)
        
        logger.info(f"Total facts inserted: {total_inserted}")


async def fetch_and_load_features(etl: IncrementalETL, missing_uuids):
    """Fetch features and update using DataLoader"""
    if not missing_uuids:
        logger.info("No features to fetch")
        return
    
    logger.info(f"Fetching {len(missing_uuids)} features...")
    
    loader = DataLoader()
    
    async with aiohttp.ClientSession() as session:
        service = SoundchartsService(session, etl.app_id, etl.api_key)
        df_features = await service.fetch_audio_features(missing_uuids)
    
    if len(df_features) > 0:
        loader.update_track_features(df_features)


async def fetch_missing_weather(missing_dates):
    """Fetch weather using DataLoader"""
    if not missing_dates:
        logger.info("No weather to fetch")
        return
    
    logger.info(f"Fetching weather for {len(missing_dates)} dates...")
    
    # Group into contiguous ranges
    ranges = []
    if missing_dates:
        start = missing_dates[0]
        end = missing_dates[0]
        
        for date in missing_dates[1:]:
            if (date - end).days == 1:
                end = date
            else:
                ranges.append((start, end))
                start = date
                end = date
        ranges.append((start, end))
    
    logger.info(f"Consolidated into {len(ranges)} ranges")
    
    loader = DataLoader(batch_size=BATCH_SIZE_WEATHER)
    
    async with aiohttp.ClientSession() as session:
        for start, end in ranges:
            logger.info(f"  Fetching {start} to {end}")
            
            weather_service = WeatherService(
                session=session,
                start_date=start.isoformat(),
                end_date=end.isoformat()
            )
            
            await loader.load_weather(weather_service)
            await asyncio.sleep(5)


async def main():
    """Main incremental ETL orchestration"""
    logger.info("="*70)
    logger.info("INCREMENTAL ETL")
    logger.info("="*70)
    logger.info(f"Timestamp: {datetime.now()}")
    
    etl = IncrementalETL()
    
    # Identify missing
    missing_weather = etl.get_missing_weather_dates()
    missing_charts = etl.get_missing_chart_dates()
    missing_features = etl.get_missing_features()
    
    # Fetch and load (uses DataLoader service)
    await fetch_missing_weather(missing_weather)
    await fetch_and_load_charts(etl, missing_charts)
    await fetch_and_load_features(etl, missing_features)
    
    logger.info("="*70)
    logger.info("INCREMENTAL ETL COMPLETE")
    logger.info("="*70)

if __name__ == "__main__":
    asyncio.run(main())