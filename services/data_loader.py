from typing import Dict, List
import json
import pandas as pd
from sqlalchemy.orm import Session
from db.models import DimTime, DimWeather, DimTrack, FactTrackChart
from db.database import SessionLocal
import logging

logger = logging.getLogger(__name__)

class DataLoader:
    """Service for loading data into database"""
    
    def __init__(self, batch_size: int = 500):
        self.batch_size = batch_size
        
    async def load_weather(self, weather_service):
        """Load averaged weather data for Germany"""
        db: Session = SessionLocal()
        batch = []
        total = 0
        
        date_lookup = self._get_date_lookup(db)
        
        try:
            async for record in weather_service.fetch_all():
                date_id = date_lookup.get(record["date"])
                if not date_id:
                    logger.warning(f"Date {record['date']} not in dim_time, skipping")
                    continue
                
                weather = DimWeather(
                    date_id=date_id,
                    temperature_avg=record["temperature_avg"],
                    precipitation_mm=record["precipitation_mm"],
                    wind_speed_kmh=record["wind_speed_kmh"],
                    sunshine_hours=record["sunshine_hours"]
                )
                batch.append(weather)
                
                if len(batch) >= self.batch_size:
                    db.bulk_save_objects(batch)
                    db.commit()
                    total += len(batch)
                    logger.info(f"Inserted {total} weather records")
                    batch = []
            
            if batch:
                db.bulk_save_objects(batch)
                db.commit()
                total += len(batch)
            
            logger.info(f"Total weather records inserted: {total}")
            
        finally:
            db.close()

    def load_tracks_bulk(self, features_df: pd.DataFrame):
        """Bulk load tracks from features DataFrame"""
        db = SessionLocal()
        
        try:
            logger.info(f"Loading {len(features_df)} tracks...")
            
            track_records = []
            
            for _, row in features_df.iterrows():
                genre = None
                if pd.notna(row.get('genres')):
                    try:
                        genres_list = json.loads(row['genres'])
                        if genres_list and len(genres_list) > 0:
                            genre = genres_list[0].get('root', '')
                    except:
                        pass
                
                duration_ms = None
                if pd.notna(row.get('duration')):
                    duration_ms = int(row['duration'] * 1000)
                
                track = DimTrack(
                    track_id=row['song_uuid'],
                    track_name=row.get('song_name', 'Unknown'),
                    artist_names=row.get('artist_name', 'Unknown'),
                    genre=genre,
                    duration_ms=duration_ms,
                    release_date=row.get('release_date'),
                    language_code=row.get('language_code'),
                    image_url=row.get('image_url'),
                    danceability=row.get('danceability'),
                    energy=row.get('energy'),
                    valence=row.get('valence'),
                    tempo=row.get('tempo'),
                    loudness=row.get('loudness'),
                    speechiness=row.get('speechiness'),
                    acousticness=row.get('acousticness'),
                    instrumentalness=row.get('instrumentalness'),
                    liveness=row.get('liveness'),
                    key=int(row['key']) if pd.notna(row.get('key')) else None,
                    mode=int(row['mode']) if pd.notna(row.get('mode')) else None,
                    time_signature=int(row['time_signature']) if pd.notna(row.get('time_signature')) else None
                )
                track_records.append(track)
            
            db.bulk_save_objects(track_records)
            db.commit()
            logger.info(f"Inserted {len(track_records)} tracks")
            
            return len(track_records)
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error loading tracks: {e}")
            raise
        finally:
            db.close()
    
    
    def load_facts_bulk(self, charts_df: pd.DataFrame):
        """Bulk load facts from charts DataFrame"""
        db = SessionLocal()
        
        try:
            date_lookup = {d.date: d.date_id for d in db.query(DimTime.date_id, DimTime.date).all()}
            weather_lookup = {w.date_id: w.weather_id for w in db.query(DimWeather.date_id, DimWeather.weather_id).all()}
            
            logger.info(f"Loaded {len(date_lookup)} dates")
            logger.info(f"Loaded {len(weather_lookup)} weather records")
            
            charts_df['date_only'] = pd.to_datetime(charts_df['chart_date']).dt.date
            charts_df['date_id'] = charts_df['date_only'].map(date_lookup)
            
            valid_facts = charts_df[charts_df['date_id'].notna()].copy()
            skipped = len(charts_df) - len(valid_facts)
            
            if skipped > 0:
                logger.warning(f"Skipping {skipped} rows without matching dates")
            
            logger.info(f"Creating {len(valid_facts)} fact records...")
            
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
            for i in range(0, len(fact_records), self.batch_size):
                batch = fact_records[i:i+self.batch_size]
                db.bulk_save_objects(batch)
                db.flush()
                
                if (i + self.batch_size) % 10000 == 0:
                    logger.info(f"Progress: {i+self.batch_size}/{len(fact_records)}")
            
            db.commit()
            logger.info(f"Inserted {len(fact_records)} facts")
            
            return len(fact_records)
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error loading facts: {e}")
            raise
        finally:
            db.close()
    
    def load_charts(self, chart_items: List[Dict], date_obj, create_tracks: bool = True):
        """Load chart data and return IDs of newly created tracks"""
        db = SessionLocal()
        
        try:
            logger.info(f"Loading {len(chart_items)} chart items for {date_obj}")
            
            date_lookup = {d.date: d.date_id for d in db.query(DimTime.date_id, DimTime.date).all()}
            weather_lookup = {w.date_id: w.weather_id for w in db.query(DimWeather.date_id, DimWeather.weather_id).all()}
            
            date_id = date_lookup.get(date_obj)
            if not date_id:
                logger.error(f"No date_id for {date_obj}")
                return 0, [] 
            
            track_ids = {item.get('song', {}).get('uuid') for item in chart_items if item.get('song', {}).get('uuid')}
            
            new_track_ids = []  
            
            if create_tracks and track_ids:
                existing_tracks = {t.track_id for t in db.query(DimTrack.track_id).filter(
                    DimTrack.track_id.in_(track_ids)
                ).all()}
                
                new_tracks = track_ids - existing_tracks
                
                if new_tracks:
                    track_records = []
                    for item in chart_items:
                        song_uuid = item.get('song', {}).get('uuid')
                        if song_uuid in new_tracks:
                            track_records.append(DimTrack(
                                track_id=song_uuid,
                                track_name=item.get('song', {}).get('name', 'Unknown'),
                                artist_names=item.get('song', {}).get('creditName', 'Unknown')
                            ))
                            new_track_ids.append(song_uuid)  # ← Collect new IDs
                    
                    db.bulk_save_objects(track_records)
                    db.commit()
                    logger.info(f"Created {len(track_records)} placeholder tracks")
            
            fact_records = []
            for item in chart_items:
                song_uuid = item.get('song', {}).get('uuid')
                if not song_uuid:
                    continue
                
                fact = FactTrackChart(
                    track_id=song_uuid,
                    date_id=date_id,
                    weather_id=weather_lookup.get(date_id),
                    country='de',
                    stream_count=item.get('metric'),
                    chart_position=item.get('position')
                )
                fact_records.append(fact)
            
            if fact_records:
                db.bulk_save_objects(fact_records)
                db.commit()
                logger.info(f"Inserted {len(fact_records)} facts for {date_obj}")
                return len(fact_records), new_track_ids  
            
            return 0, []
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error: {e}")
            raise
        finally:
            db.close()
    
    def update_track_features(self, features_df: pd.DataFrame):
        """Update DimTrack with audio features"""
        db = SessionLocal()
        updated = 0
        
        try:
            for _, row in features_df.iterrows():
                track = db.query(DimTrack).filter(DimTrack.track_id == row['song_uuid']).first()
                
                if track:
                    if pd.notna(row.get('song_name')):
                        track.track_name = row['song_name']
                    if pd.notna(row.get('artist_name')):
                        track.artist_names = row['artist_name']
                    
                    track.danceability = row.get('danceability')
                    track.energy = row.get('energy')
                    track.valence = row.get('valence')
                    track.tempo = row.get('tempo')
                    track.loudness = row.get('loudness')
                    track.speechiness = row.get('speechiness')
                    track.acousticness = row.get('acousticness')
                    track.instrumentalness = row.get('instrumentalness')
                    track.liveness = row.get('liveness')
                    track.key = int(row['key']) if pd.notna(row.get('key')) else None
                    track.mode = int(row['mode']) if pd.notna(row.get('mode')) else None
                    track.time_signature = int(row['time_signature']) if pd.notna(row.get('time_signature')) else None
                    track.duration_ms = int(row['duration'] * 1000) if pd.notna(row.get('duration')) else None
                    track.release_date = row.get('release_date')
                    track.language_code = row.get('language_code')
                    track.image_url = row.get('image_url')
                    
                    if pd.notna(row.get('genres')):
                        try:
                            genres_list = json.loads(row['genres'])
                            if genres_list and len(genres_list) > 0:
                                track.genre = genres_list[0].get('root', '')
                        except:
                            pass
                    
                    updated += 1
            
            db.commit()
            logger.info(f"Updated {updated} tracks with features")
            return updated
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating features: {e}")
            raise
        finally:
            db.close()
    
    def _get_date_lookup(self, db: Session) -> Dict[str, int]:
        """Create lookup dict: date_string -> date_id"""
        dates = db.query(DimTime.date_id, DimTime.date).all()
        return {d.date.isoformat(): d.date_id for d in dates}