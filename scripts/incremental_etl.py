"""
Incremental ETL - Fetch only missing/new data
Uses existing services - no code duplication
"""
import asyncio
import aiohttp
import pandas as pd
import os
import logging
from datetime import datetime

from services.soundcharts_service import SoundchartsService
from services.weather_service import WeatherService
from services.data_loader import DataLoader
from config import CHARTS_CSV, FEATURES_CSV, BATCH_SIZE_WEATHER
from db.database import SessionLocal
from db.models import DimTime, DimWeather

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IncrementalETL:
    """Identifies missing data and orchestrates fetching"""
    
    def __init__(self):
        self.app_id = os.getenv('SOUNDCHARTS_APP_ID')
        self.api_key = os.getenv('SOUNDCHARTS_API_KEY')
        
        if not self.app_id or not self.api_key:
            raise ValueError("Missing SOUNDCHARTS_APP_ID or SOUNDCHARTS_API_KEY")
    
    def get_missing_chart_dates(self):
        """Find chart dates missing from CSV"""
        db = SessionLocal()
        
        # All Sundays in DimTime
        all_dates = {d.date for d in db.query(DimTime.date).filter(
            DimTime.date.isnot(None)
        ).all()}
        sundays = {d for d in all_dates if d.weekday() == 6}
        
        # Dates with charts
        if os.path.exists(CHARTS_CSV):
            charts_df = pd.read_csv(CHARTS_CSV)
            charts_df['chart_date'] = pd.to_datetime(charts_df['chart_date']).dt.date
            chart_dates = set(charts_df['chart_date'].unique())
        else:
            chart_dates = set()
        
        db.close()
        
        missing = sorted(list(sundays - chart_dates))
        logger.info(f"Missing chart dates: {len(missing)}")
        
        return missing
    
    def get_missing_features(self):
        """Find songs without features"""
        if not os.path.exists(CHARTS_CSV):
            return []
        
        charts_df = pd.read_csv(CHARTS_CSV)
        all_songs = set(charts_df['song_uuid'].unique())
        
        if os.path.exists(FEATURES_CSV):
            features_df = pd.read_csv(FEATURES_CSV)
            have_features = set(features_df['song_uuid'].unique())
        else:
            have_features = set()
        
        missing = list(all_songs - have_features)
        logger.info(f"Missing features: {len(missing)}")
        
        # Limit to 200 per run (API quota)
        return missing[:200]
    
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
    
    def load_new_data(self):
        """Load new data into database using existing loader"""
        logger.info("Loading new data into database...")
        from scripts.s05_load_soundcharts import load_soundcharts_data
        load_soundcharts_data()


async def fetch_missing_charts(etl: IncrementalETL, missing_dates):
    """Fetch charts using existing ChartsFetcher logic"""
    if not missing_dates:
        logger.info("No charts to fetch")
        return
    
    logger.info(f"Fetching {len(missing_dates)} charts...")
    
    # Import and reuse ChartsFetcher
    from scripts.s03_fetch_charts import ChartsFetcher
    
    async with aiohttp.ClientSession() as session:
        service = SoundchartsService(session, etl.app_id, etl.api_key)
        fetcher = ChartsFetcher()
        
        # Get API dates
        all_api_dates = await fetcher.get_chart_dates_from_api(service)
        
        # Filter to missing only
        to_fetch = [(d, api_str) for d, api_str in all_api_dates if d in missing_dates]
        
        if to_fetch:
            await fetcher.fetch_batch(service, to_fetch)
            logger.info(f"Fetched {len(to_fetch)} charts")


async def fetch_missing_features(etl: IncrementalETL, missing_uuids):
    """Fetch features using existing ResumableFetcher logic"""
    if not missing_uuids:
        logger.info("No features to fetch")
        return
    
    logger.info(f"Fetching {len(missing_uuids)} features...")
    
    # Import and reuse ResumableFetcher
    from scripts.soundcharts.fetch_track_features import ResumableFetcher
    
    async with aiohttp.ClientSession() as session:
        service = SoundchartsService(session, etl.app_id, etl.api_key)
        fetcher = ResumableFetcher()
        
        await fetcher.fetch_batch(service, missing_uuids)
        logger.info(f"Fetched {len(missing_uuids)} features")


async def fetch_missing_weather(missing_dates):
    """Fetch weather using existing WeatherService"""
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
    
    # Reuse WeatherService + DataLoader
    async with aiohttp.ClientSession() as session:
        for start, end in ranges:
            logger.info(f"  Fetching {start} to {end}")
            
            weather_service = WeatherService(
                session=session,
                start_date=start.isoformat(),
                end_date=end.isoformat()
            )
            
            loader = DataLoader(batch_size=BATCH_SIZE_WEATHER)
            await loader.load_weather(weather_service)
            
            await asyncio.sleep(5)


async def main():
    """Main incremental ETL orchestration"""
    logger.info("="*70)
    logger.info("INCREMENTAL ETL")
    logger.info("="*70)
    logger.info(f"Timestamp: {datetime.now()}")
    
    etl = IncrementalETL()
    
    # 1. Identify missing data
    missing_weather = etl.get_missing_weather_dates()
    missing_charts = etl.get_missing_chart_dates()
    missing_features = etl.get_missing_features()
    
    # 2. Fetch missing (weather first - needed for facts)
    await fetch_missing_weather(missing_weather)
    await fetch_missing_charts(etl, missing_charts)
    await fetch_missing_features(etl, missing_features)
    
    # 3. Load into DB
    if missing_charts or missing_features:
        etl.load_new_data()
    
    logger.info("="*70)
    logger.info("INCREMENTAL ETL COMPLETE")
    logger.info("="*70)

if __name__ == "__main__":
    asyncio.run(main())