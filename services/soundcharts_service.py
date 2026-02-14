"""
Soundcharts API Service
Reusable service for fetching Spotify DE Top 200 charts with audio features
"""
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime
import json
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class SoundchartsService:
    """Service for fetching and processing Soundcharts data"""
    
    def __init__(self, session: aiohttp.ClientSession, app_id: str, api_key: str):
        self.session = session
        self.app_id = app_id
        self.api_key = api_key
        self.base_url = "https://customer.api.soundcharts.com/api/v2.14"
        self.ranking_dates_url = "https://customer.api.soundcharts.com/api/v2"
        self.metadata_base_url = "https://customer.api.soundcharts.com/api/v2.25"
        self.headers = {
            'x-app-id': app_id,
            'x-api-key': api_key
        }
        self.request_count = 0
    
    async def fetch_available_chart_dates(self, slug: str, max_offset: int = 500) -> List[str]:
        """Fetch all available chart ranking dates from API"""
        url = f"{self.ranking_dates_url}/chart/song/{slug}/available-rankings"
        all_dates = []
        limit = 100
        
        logger.info(f"Fetching available chart dates for {slug}...")
        
        for offset in range(0, max_offset + 1, limit):
            params = {'offset': offset, 'limit': limit}
            
            try:
                async with self.session.get(url, params=params, headers=self.headers) as response:
                    self.request_count += 1
                    
                    if response.status == 200:
                        data = await response.json()
                        items = data.get('items', [])
                        
                        if items:
                            all_dates.extend(items)
                            logger.info(f"  Offset {offset}: {len(items)} dates")
                        else:
                            logger.info(f"  Offset {offset}: No more dates")
                            break
                    elif response.status == 404:
                        break
                    else:
                        logger.error(f"  Offset {offset}: Error {response.status}")
                        break
                    
                    await asyncio.sleep(0.3)
                    
            except Exception as e:
                logger.error(f"  Offset {offset}: {e}")
                break
        
        unique_dates = list(dict.fromkeys(all_dates))
        logger.info(f"Found {len(unique_dates)} unique chart dates")
        
        return unique_dates
    
    async def fetch_chart_page(self, slug: str, date_str: str, offset: int) -> List[Dict]:
        """Fetch one page (max 100 items) for a specific chart and date"""
        url = f"{self.base_url}/chart/song/{slug}/ranking/{date_str}"
        params = {'offset': offset, 'limit': 100}
        
        try:
            async with self.session.get(url, params=params, headers=self.headers) as response:
                self.request_count += 1
                
                if response.status == 200:
                    data = await response.json()
                    return data.get('items', [])
                else:
                    logger.error(f"Error {response.status} for {date_str[:10]} offset {offset}")
                    return []
        except Exception as e:
            logger.error(f"Exception: {e}")
            return []
    
    async def fetch_chart_for_date(self, slug: str, date_str: str, top_n: int = 200) -> List[Dict]:
        """Fetch top N chart positions for a specific date"""
        pages_needed = (top_n + 99) // 100
        tasks = [self.fetch_chart_page(slug, date_str, page * 100) for page in range(pages_needed)]
        
        results = await asyncio.gather(*tasks)
        all_items = [item for sublist in results for item in sublist]
        
        return all_items[:top_n]
    
    def flatten_chart_item(self, item: Dict, chart_date: str) -> Dict:
        """Flatten chart item to flat structure for CSV"""
        return {
            'chart_date': chart_date,
            'fetched_at': datetime.now().isoformat(),
            'position': item.get('position'),
            'old_position': item.get('oldPosition'),
            'position_change': item.get('positionEvolution'),
            'streams': item.get('metric'),
            'entry_state': item.get('entryState'),
            'entry_date': item.get('entryDate'),
            'rank_date': item.get('rankDate'),
            'weeks_on_chart': item.get('timeOnChart'),
            'time_unit': item.get('timeOnChartUnit'),
            'song_uuid': item.get('song', {}).get('uuid'),
            'song_name': item.get('song', {}).get('name'),
            'artist_name': item.get('song', {}).get('creditName'),
            'image_url': item.get('song', {}).get('imageUrl'),
            'raw_json': json.dumps(item)
        }
    
    async def fetch_audio_features(self, uuids: List[str]) -> pd.DataFrame:
        """Fetch audio features for list of song UUIDs"""
        logger.info(f"Fetching audio features for {len(uuids)} songs...")
        
        features = []
        not_found_count = 0
        error_count = 0
        
        for i, uuid in enumerate(uuids, 1):
            if i % 50 == 0 or i == len(uuids):
                logger.info(f"Progress: {i}/{len(uuids)} ({len(features)} success, {not_found_count} not found)")
            
            url = f"{self.metadata_base_url}/song/{uuid}"
            
            try:
                async with self.session.get(url, headers=self.headers) as response:
                    self.request_count += 1
                    
                    if response.status == 200:
                        data = await response.json()
                        obj = data.get('object', {})
                        audio = obj.get('audio', {})
                        
                        if audio and any(v is not None for v in audio.values()):
                            features.append({
                                'song_uuid': uuid,
                                'song_name': obj.get('name'),
                                'artist_name': obj.get('creditName'),
                                'isrc': obj.get('isrc', {}).get('value') if isinstance(obj.get('isrc'), dict) else None,
                                'release_date': obj.get('releaseDate'),
                                'duration': obj.get('duration'),
                                'explicit': obj.get('explicit'),
                                'language_code': obj.get('languageCode'),
                                'acousticness': audio.get('acousticness'),
                                'danceability': audio.get('danceability'),
                                'energy': audio.get('energy'),
                                'instrumentalness': audio.get('instrumentalness'),
                                'key': audio.get('key'),
                                'liveness': audio.get('liveness'),
                                'loudness': audio.get('loudness'),
                                'mode': audio.get('mode'),
                                'speechiness': audio.get('speechiness'),
                                'tempo': audio.get('tempo'),
                                'time_signature': audio.get('timeSignature'),
                                'valence': audio.get('valence'),
                                'image_url': obj.get('imageUrl'),
                                'genres': json.dumps(obj.get('genres', [])),
                                'copyright': obj.get('copyright')
                            })
                        else:
                            not_found_count += 1
                    elif response.status == 404:
                        not_found_count += 1
                    else:
                        error_count += 1
                
                await asyncio.sleep(0.1)
                
            except Exception as e:
                if error_count < 10:
                    logger.error(f"Exception for {uuid}: {e}")
                error_count += 1
        
        success_rate = len(features) / len(uuids) * 100 if len(uuids) > 0 else 0
        logger.info(f"Complete: {len(features)}/{len(uuids)} ({success_rate:.1f}% success)")
        logger.info(f"Requests used: {self.request_count}")
        
        return pd.DataFrame(features)