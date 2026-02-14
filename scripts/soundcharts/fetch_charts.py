"""
Fetch Soundcharts weekly Top 200 charts for Germany
- Fetches available dates from API
- Filters by config date range
- Saves to CSV with incremental progress
- Resumable if interrupted
"""
import pandas as pd
import asyncio
import aiohttp
import os
import logging
from datetime import datetime

from services.soundcharts_service import SoundchartsService
from config import CHART_START_DATE, CHART_END_DATE, CHARTS_CSV, CHARTS_BATCH_SIZE

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ChartsFetcher:
    def __init__(self):
        self.output_file = CHARTS_CSV
        self.start_date = CHART_START_DATE
        self.end_date = CHART_END_DATE
        self.batch_size = CHARTS_BATCH_SIZE
        self.chart_slug = 'top-songs-22'
    
    def parse_api_date(self, date_str: str) -> datetime.date:
        """Parse API date string to date object"""
        dt = datetime.fromisoformat(date_str.replace('+00:00', ''))
        return dt.date()
    
    async def get_chart_dates_from_api(self, service: SoundchartsService):
        """Get all available chart dates from API, filtered by config range"""
        all_api_dates = await service.fetch_available_chart_dates(self.chart_slug)
        
        filtered = []
        for date_str in all_api_dates:
            date_obj = self.parse_api_date(date_str)
            if self.start_date <= date_obj <= self.end_date:
                filtered.append((date_obj, date_str))
        
        filtered.sort(key=lambda x: x[0])
        
        logger.info(f"Date filter: {self.start_date} to {self.end_date}")
        logger.info(f"API total: {len(all_api_dates)}, In range: {len(filtered)}")
        
        return filtered
    
    def load_existing_progress(self):
        """Load already fetched chart dates"""
        if os.path.exists(self.output_file):
            df = pd.read_csv(self.output_file)
            df['chart_date'] = pd.to_datetime(df['chart_date']).dt.date
            fetched_dates = set(df['chart_date'].unique())
            logger.info(f"Found existing charts: {len(fetched_dates)} weeks")
            return fetched_dates, df
        else:
            logger.info("No existing charts file, starting fresh")
            return set(), pd.DataFrame()
    
    def get_remaining_dates(self, all_dates, fetched_dates):
        """Get dates that still need fetching"""
        return [(d, api) for d, api in all_dates if d not in fetched_dates]
    
    def save_progress(self, df_new: pd.DataFrame, df_existing: pd.DataFrame):
        """Append new charts to file"""
        if len(df_new) == 0:
            return
        
        df_combined = pd.concat([df_existing, df_new], ignore_index=True) if len(df_existing) > 0 else df_new
        df_combined['chart_date'] = pd.to_datetime(df_combined['chart_date']).dt.strftime('%Y-%m-%d')
        df_combined = df_combined.sort_values(['chart_date', 'position'])
        
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        df_combined.to_csv(self.output_file, index=False)
        
        total_weeks = len(df_combined['chart_date'].unique())
        logger.info(f"Progress saved: {total_weeks} weeks, {len(df_combined):,} entries")
    
    async def fetch_week(self, service: SoundchartsService, date_obj, api_date_str):
        """Fetch one week's chart"""
        try:
            items = await service.fetch_chart_for_date(self.chart_slug, api_date_str, top_n=200)
            
            if len(items) > 0:
                flattened = [service.flatten_chart_item(item, api_date_str) for item in items]
                df = pd.DataFrame(flattened)
                df['chart_date'] = date_obj
                logger.info(f"  {date_obj}: {len(df)} songs")
                return df
            else:
                logger.warning(f"  {date_obj}: No data")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"  {date_obj}: {e}")
            return pd.DataFrame()
    
    async def fetch_batch(self, service: SoundchartsService, dates: list):
        """Fetch charts in batches with progress saving"""
        fetched_dates, df_existing = self.load_existing_progress()
        total = len(dates)
        total_fetched = 0
        
        logger.info(f"Fetching {total} weeks in batches of {self.batch_size}")
        
        for i in range(0, total, self.batch_size):
            batch = dates[i:i+self.batch_size]
            batch_num = i // self.batch_size + 1
            total_batches = (total + self.batch_size - 1) // self.batch_size
            
            logger.info(f"Batch {batch_num}/{total_batches} ({len(batch)} weeks)")
            
            batch_data = []
            for date_obj, api_str in batch:
                df_week = await self.fetch_week(service, date_obj, api_str)
                if len(df_week) > 0:
                    batch_data.append(df_week)
                await asyncio.sleep(0.5)
            
            if batch_data:
                df_batch = pd.concat(batch_data, ignore_index=True)
                self.save_progress(df_batch, df_existing)
                
                df_existing = pd.read_csv(self.output_file)
                df_existing['chart_date'] = pd.to_datetime(df_existing['chart_date'])
                
                total_fetched += len(batch_data)
                logger.info(f"Session total: {total_fetched}/{total} weeks")
                logger.info(f"Requests used: {service.request_count}")
            
            if service.request_count >= 950:
                logger.warning(f"Approaching request limit ({service.request_count}/1000)")
                break
        
        return total_fetched, service.request_count


async def main_fetch_charts(app_id: str, api_key: str):
    """Main charts fetching function"""
    fetcher = ChartsFetcher()
    
    async with aiohttp.ClientSession() as session:
        service = SoundchartsService(session, app_id, api_key)
        
        all_dates = await fetcher.get_chart_dates_from_api(service)
        fetched_dates, _ = fetcher.load_existing_progress()
        remaining = fetcher.get_remaining_dates(all_dates, fetched_dates)
        
        logger.info(f"Total weeks: {len(all_dates)}")
        logger.info(f"Already fetched: {len(fetched_dates)}")
        logger.info(f"Remaining: {len(remaining)}")
        
        if len(remaining) == 0:
            logger.info("All charts already fetched")
            return
        
        logger.info(f"This will use approximately {len(remaining) * 2} API requests")
        response = input("Continue? (y/n): ")
        if response.lower() != 'y':
            logger.info("Aborted")
            return
        
        fetched, requests_used = await fetcher.fetch_batch(service, remaining)
        
        logger.info(f"Session complete: {fetched} weeks fetched, {requests_used} requests used")
        logger.info(f"Total progress: {len(fetched_dates) + fetched}/{len(all_dates)}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch Soundcharts weekly charts')
    parser.add_argument('--app-id', required=True, help='Soundcharts app ID')
    parser.add_argument('--api-key', required=True, help='Soundcharts API key')
    
    args = parser.parse_args()
    
    asyncio.run(main_fetch_charts(args.app_id, args.api_key))