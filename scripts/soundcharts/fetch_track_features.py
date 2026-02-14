"""
Fetch audio features for chart songs (resumable)
- Starts with newest songs first
- Saves progress after each batch
- Can resume if interrupted
"""
import pandas as pd
import asyncio
import aiohttp
import os
import logging

from services.soundcharts_service import SoundchartsService
from config import CHARTS_CSV, FEATURES_CSV, FEATURES_BATCH_SIZE

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ResumableFetcher:
    def __init__(self):
        self.charts_csv = CHARTS_CSV
        self.progress_file = FEATURES_CSV
        self.batch_size = FEATURES_BATCH_SIZE
    
    def get_prioritized_uuids(self):
        """Get all UUIDs sorted by newest appearance first"""
        logger.info("Analyzing charts and prioritizing songs...")
        
        df = pd.read_csv(self.charts_csv)
        df['chart_date'] = pd.to_datetime(df['chart_date'])
        
        logger.info(f"Total entries: {len(df):,}")
        logger.info(f"Date range: {df['chart_date'].min().date()} to {df['chart_date'].max().date()}")
        logger.info(f"Unique songs: {df['song_uuid'].nunique():,}")
        
        song_latest = df.groupby('song_uuid').agg({
            'chart_date': 'max',
            'position': 'min',
            'streams': 'sum'
        }).reset_index()
        
        song_latest = song_latest.sort_values(['chart_date', 'position'], ascending=[False, True])
        
        return song_latest['song_uuid'].tolist()
    
    def load_progress(self):
        """Load already fetched UUIDs"""
        if os.path.exists(self.progress_file):
            df = pd.read_csv(self.progress_file)
            fetched = set(df['song_uuid'].tolist())
            logger.info(f"Loaded progress: {len(fetched)} songs already fetched")
            return fetched
        else:
            logger.info("No progress file found, starting fresh")
            return set()
    
    def get_remaining_uuids(self, all_uuids, fetched_uuids):
        """Get UUIDs that still need fetching"""
        return [u for u in all_uuids if u not in fetched_uuids]
    
    def save_batch(self, df_new: pd.DataFrame):
        """Append newly fetched features to progress file"""
        if len(df_new) == 0:
            return
        
        if os.path.exists(self.progress_file):
            df_existing = pd.read_csv(self.progress_file)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=['song_uuid'], keep='first')
        else:
            df_combined = df_new
        
        os.makedirs(os.path.dirname(self.progress_file), exist_ok=True)
        df_combined.to_csv(self.progress_file, index=False)
        logger.info(f"Progress saved: {len(df_combined)} total features")
    
    async def fetch_batch(self, service: SoundchartsService, uuids: list):
        """Fetch features in batches with progress saving"""
        total = len(uuids)
        total_fetched = 0
        
        logger.info(f"Fetching {total} songs in batches of {self.batch_size}")
        
        for i in range(0, total, self.batch_size):
            batch = uuids[i:i+self.batch_size]
            batch_num = i // self.batch_size + 1
            total_batches = (total + self.batch_size - 1) // self.batch_size
            
            logger.info(f"Batch {batch_num}/{total_batches} ({len(batch)} songs)")
            
            try:
                df_batch = await service.fetch_audio_features(batch)
                
                if len(df_batch) > 0:
                    self.save_batch(df_batch)
                    total_fetched += len(df_batch)
                    logger.info(f"Session total: {total_fetched}/{total}")
                    logger.info(f"Requests used: {service.request_count}")
                
                if service.request_count >= 950:
                    logger.warning(f"Approaching request limit ({service.request_count}/1000)")
                    break
                
            except Exception as e:
                logger.error(f"Error in batch {batch_num}: {e}")
                break
        
        return total_fetched, service.request_count


async def main_fetch(app_id: str, api_key: str, max_requests: int = None):
    """Main fetching function"""
    fetcher = ResumableFetcher()
    
    all_uuids = fetcher.get_prioritized_uuids()
    already_fetched = fetcher.load_progress()
    remaining_uuids = fetcher.get_remaining_uuids(all_uuids, already_fetched)
    
    logger.info(f"Total songs: {len(all_uuids):,}")
    logger.info(f"Already fetched: {len(already_fetched):,}")
    logger.info(f"Remaining: {len(remaining_uuids):,}")
    
    if len(remaining_uuids) == 0:
        logger.info("All features already fetched")
        return
    
    if max_requests:
        to_fetch = remaining_uuids[:max_requests]
        logger.info(f"Will fetch: {len(to_fetch):,} (limited by max={max_requests})")
    else:
        to_fetch = remaining_uuids
        logger.info(f"Will fetch: {len(to_fetch):,}")
    
    logger.info(f"This will use approximately {len(to_fetch)} API requests")
    response = input("Continue? (y/n): ")
    if response.lower() != 'y':
        logger.info("Aborted")
        return
    
    async with aiohttp.ClientSession() as session:
        service = SoundchartsService(session, app_id, api_key)
        fetched, requests_used = await fetcher.fetch_batch(service, to_fetch)
    
    logger.info(f"Session complete: {fetched:,} songs fetched, {requests_used:,} requests used")
    logger.info(f"Total progress: {len(already_fetched) + fetched:,}/{len(all_uuids):,}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch audio features (resumable)')
    parser.add_argument('--app-id', required=True, help='Soundcharts app ID')
    parser.add_argument('--api-key', required=True, help='Soundcharts API key')
    parser.add_argument('--max', type=int, help='Max requests (optional)')
    
    args = parser.parse_args()
    
    asyncio.run(main_fetch(args.app_id, args.api_key, args.max))