from sqlalchemy import func, case, or_
from sqlalchemy.orm import Session
from db.database import SessionLocal
from db.models import FactTrackChart, DimTrack, DimTime, DimWeather
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class SoundOfSeasonsStats:
    def __init__(self):
        self.db: Session = SessionLocal()
    
    def get_kpis(self, country: str = None) -> dict:
        """KPIs - ohne Feiertage"""
        query_base = self.db.query(FactTrackChart)
        
        if country:
            query_base = query_base.filter(FactTrackChart.country == country)
        
        total_streams = query_base.with_entities(
            func.sum(FactTrackChart.stream_count)
        ).scalar() or 0
        
        unique_tracks = query_base.with_entities(
            func.count(func.distinct(FactTrackChart.track_id))
        ).scalar() or 0
        
        avg_temp = self.db.query(
            func.avg(DimWeather.temperature_avg)
        ).scalar() or 0
        
        # Feature coverage
        if country:
            tracks_with_features = self.db.query(
                func.count(func.distinct(DimTrack.track_id))
            ).join(
                FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
            ).filter(
                DimTrack.danceability.isnot(None),
                FactTrackChart.country == country
            ).scalar() or 0
        else:
            tracks_with_features = self.db.query(
                func.count(func.distinct(DimTrack.track_id))
            ).join(
                FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
            ).filter(
                DimTrack.danceability.isnot(None)
            ).scalar() or 0
        
        # Date range
        date_range = self.db.query(
            func.min(DimTime.date),
            func.max(DimTime.date)
        ).join(
            FactTrackChart, DimTime.date_id == FactTrackChart.date_id
        ).first()
    
        return {
            'total_streams': f"{int(total_streams/1_000_000):.1f}M" if total_streams > 1_000_000 else f"{int(total_streams/1_000):.1f}K",
            'unique_tracks': unique_tracks,
            'avg_temp': f"{avg_temp:.1f}",
            'feature_coverage': f"{(tracks_with_features/unique_tracks*100):.0f}" if unique_tracks > 0 else "0",
            'date_range': f"{date_range[0]} - {date_range[1]}" if date_range else "N/A"
        }

    def get_current_top_tracks(self, country: str = 'de', limit: int = 3) -> pd.DataFrame:
        """
        Current week's top 3 tracks 
        Returns: track_name, artist, image_url, streams, position
        """
        # Get latest Sunday
        latest_date = self.db.query(
            func.max(DimTime.date)
        ).join(
            FactTrackChart, DimTime.date_id == FactTrackChart.date_id
        ).filter(
            FactTrackChart.country == country
        ).scalar()
        
        if not latest_date:
            return pd.DataFrame()
        
        # Current week top 3
        query = self.db.query(
            DimTrack.track_name,
            DimTrack.artist_names,
            DimTrack.image_url,
            FactTrackChart.stream_count.label('streams'),
            FactTrackChart.chart_position.label('position')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            DimTime.date == latest_date,
            FactTrackChart.country == country
        ).order_by(
            FactTrackChart.chart_position
        ).limit(limit)
        
        df = pd.read_sql(query.statement, self.db.bind)
        
        if not df.empty:
            df['chart_date'] = latest_date
        
        return df

    def get_weekly_feature_changes(self, country: str = 'de') -> dict:
        """
        Vergleich durchschnittliche Audio Features + Wetter: Diese Woche vs Letzte Woche
        """
        from datetime import timedelta
        
        # Latest Sunday (Chart-Datum)
        latest_chart_date = self.db.query(
            func.max(DimTime.date)
        ).join(
            FactTrackChart, DimTime.date_id == FactTrackChart.date_id
        ).filter(
            FactTrackChart.country == country
        ).scalar()
        
        if not latest_chart_date:
            return {}
        
        previous_chart_date = latest_chart_date - timedelta(days=7)
        
        # Wetter-Perioden: 7 Tage vor Chart
        current_weather_end = latest_chart_date
        current_weather_start = latest_chart_date - timedelta(days=6)
        
        previous_weather_end = previous_chart_date
        previous_weather_start = previous_chart_date - timedelta(days=6)
        
        # Current week - Audio Features
        current_audio = self.db.query(
            func.avg(DimTrack.valence).label('valence'),
            func.avg(DimTrack.danceability).label('danceability'),
            func.avg(DimTrack.energy).label('energy'),
            func.avg(DimTrack.tempo).label('tempo'),
            func.avg(DimTrack.acousticness).label('acousticness')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            DimTime.date == latest_chart_date,
            FactTrackChart.country == country,
            DimTrack.valence.isnot(None)
        ).first()
        
        # Previous week - Audio Features
        previous_audio = self.db.query(
            func.avg(DimTrack.valence).label('valence'),
            func.avg(DimTrack.danceability).label('danceability'),
            func.avg(DimTrack.energy).label('energy'),
            func.avg(DimTrack.tempo).label('tempo'),
            func.avg(DimTrack.acousticness).label('acousticness')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            DimTime.date == previous_chart_date,
            FactTrackChart.country == country,
            DimTrack.valence.isnot(None)
        ).first()
        
        # Count UNIQUE songs across both charts
        unique_songs_count = self.db.query(
            func.count(func.distinct(DimTrack.track_id))
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            DimTime.date.in_([latest_chart_date, previous_chart_date]),
            FactTrackChart.country == country,
            DimTrack.valence.isnot(None)
        ).scalar()
        
        # Current week - Weather 
        current_weather = self.db.query(
            func.avg(DimWeather.temperature_avg).label('temperature'),
            func.avg(DimWeather.sunshine_hours).label('sunshine'),
            func.avg(DimWeather.precipitation_mm).label('precipitation'),
            func.avg(DimWeather.wind_speed_kmh).label('wind_speed')
        ).join(
            DimTime, DimWeather.date_id == DimTime.date_id
        ).filter(
            DimTime.date.between(current_weather_start, current_weather_end)
        ).first()
        
        # Previous week - Weather 
        previous_weather = self.db.query(
            func.avg(DimWeather.temperature_avg).label('temperature'),
            func.avg(DimWeather.sunshine_hours).label('sunshine'),
            func.avg(DimWeather.precipitation_mm).label('precipitation'),
            func.avg(DimWeather.wind_speed_kmh).label('wind_speed')
        ).join(
            DimTime, DimWeather.date_id == DimTime.date_id
        ).filter(
            DimTime.date.between(previous_weather_start, previous_weather_end)
        ).first()
        
        if not current_audio or not previous_audio or not current_weather or not previous_weather:
            return {}
        
        return {
            'chart_date': latest_chart_date,
            'previous_chart_date': previous_chart_date,
            'weather_period_current': f"{current_weather_start.strftime('%d.%m')} - {current_weather_end.strftime('%d.%m.%Y')}",
            'weather_period_previous': f"{previous_weather_start.strftime('%d.%m')} - {previous_weather_end.strftime('%d.%m.%Y')}",
            'unique_songs': unique_songs_count,
            'features': {
                'valence': {
                    'current': current_audio.valence,
                    'previous': previous_audio.valence,
                    'change': current_audio.valence - previous_audio.valence,
                    'change_pct': ((current_audio.valence - previous_audio.valence) / previous_audio.valence * 100) if previous_audio.valence else 0
                },
                'danceability': {
                    'current': current_audio.danceability,
                    'previous': previous_audio.danceability,
                    'change': current_audio.danceability - previous_audio.danceability,
                    'change_pct': ((current_audio.danceability - previous_audio.danceability) / previous_audio.danceability * 100) if previous_audio.danceability else 0
                },
                'energy': {
                    'current': current_audio.energy,
                    'previous': previous_audio.energy,
                    'change': current_audio.energy - previous_audio.energy,
                    'change_pct': ((current_audio.energy - previous_audio.energy) / previous_audio.energy * 100) if previous_audio.energy else 0
                },
                'tempo': {
                    'current': current_audio.tempo,
                    'previous': previous_audio.tempo,
                    'change': current_audio.tempo - previous_audio.tempo,
                    'change_pct': ((current_audio.tempo - previous_audio.tempo) / previous_audio.tempo * 100) if previous_audio.tempo else 0
                },
                'acousticness': {
                    'current': current_audio.acousticness,
                    'previous': previous_audio.acousticness,
                    'change': current_audio.acousticness - previous_audio.acousticness,
                    'change_pct': ((current_audio.acousticness - previous_audio.acousticness) / previous_audio.acousticness * 100) if previous_audio.acousticness else 0
                }
            },
            'weather': {
                'temperature': {
                    'current': current_weather.temperature,
                    'previous': previous_weather.temperature,
                    'change': current_weather.temperature - previous_weather.temperature
                },
                'sunshine': {
                    'current': current_weather.sunshine,
                    'previous': previous_weather.sunshine,
                    'change': current_weather.sunshine - previous_weather.sunshine
                },
                'precipitation': {
                    'current': current_weather.precipitation,
                    'previous': previous_weather.precipitation,
                    'change': current_weather.precipitation - previous_weather.precipitation
                }
            }
        }

    def get_audio_features_by_season(self, country: str = 'de') -> pd.DataFrame:
        """
        Audio Features Durchschnitt pro Jahreszeit
        Jeder Chart-Eintrag zählt (Track kann in mehreren Seasons sein)
        """
        query = self.db.query(
            DimTime.season,
            func.avg(DimTrack.danceability).label('danceability'),
            func.avg(DimTrack.energy).label('energy'),
            func.avg(DimTrack.instrumentalness).label('instrumentalness'),
            func.avg(DimTrack.loudness).label('loudness'),
            func.avg(DimTrack.valence).label('valence'),
            func.avg(DimTrack.tempo).label('tempo'),
            func.avg(DimTrack.acousticness).label('acousticness'),
            func.count(FactTrackChart.fact_id).label('chart_entries'),
            func.count(func.distinct(DimTrack.track_id)).label('unique_tracks')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            DimTrack.danceability.isnot(None)
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(DimTime.season)
        
        df = pd.read_sql(query.statement, self.db.bind)
        
        # Order seasons correctly
        season_order = {'Frühling': 0, 'Sommer': 1, 'Herbst': 2, 'Winter': 3}
        if not df.empty:
            df['season_order'] = df['season'].map(season_order)
            df = df.sort_values('season_order').drop('season_order', axis=1)
        
        return df

    def get_audio_features_by_weather(self, country: str = 'de') -> pd.DataFrame:
        """
        Audio Features nach Wetter-Kategorien 
        Wetter = Durchschnitt der 7 Tage VOR Chart-Sonntag
        
        Kategorien:
        - Regnerisch: >5mm Niederschlag
        - Sehr Sonnig: >12h Sonne
        - Sonnig: 8-12h Sonne
        """
        from datetime import timedelta, datetime
        import pandas as pd
        
        # Get all chart dates
        chart_dates_query = self.db.query(
            DimTime.date
        ).join(
            FactTrackChart, DimTime.date_id == FactTrackChart.date_id
        )
        
        if country:
            chart_dates_query = chart_dates_query.filter(FactTrackChart.country == country)
        
        chart_dates_query = chart_dates_query.distinct().order_by(DimTime.date)
        
        chart_dates = [row.date for row in chart_dates_query.all()]
        
        # For each chart date, calculate weather average of previous 7 days
        weather_data = []
        
        for chart_date in chart_dates:
            # Ensure chart_date is a date object
            if isinstance(chart_date, str):
                chart_date = datetime.strptime(chart_date, '%Y-%m-%d').date()
            
            # Weather period: 7 days before chart (including chart day)
            weather_end = chart_date
            weather_start = chart_date - timedelta(days=6)
            
            # Calculate average weather for this period
            weather_avg = self.db.query(
                func.avg(DimWeather.temperature_avg).label('avg_temperature'),
                func.avg(DimWeather.precipitation_mm).label('avg_precipitation'),
                func.avg(DimWeather.sunshine_hours).label('avg_sunshine'),
            ).join(
                DimTime, DimWeather.date_id == DimTime.date_id
            ).filter(
                DimTime.date.between(weather_start, weather_end)
            ).first()
            
            if weather_avg and weather_avg.avg_sunshine is not None:
                # Categorize based on weekly averages - 3 clear categories
                category = None
                
                if weather_avg.avg_precipitation and weather_avg.avg_precipitation > 5:
                    category = '🌧️ Regnerisch (>5mm)'
                elif weather_avg.avg_sunshine and weather_avg.avg_sunshine > 12:
                    category = '🌞 Sehr Sonnig (>12h)'
                elif weather_avg.avg_sunshine and weather_avg.avg_sunshine > 8:
                    category = '☀️ Sonnig (8-12h)'
                
                if category:  # Only add if we have a clear category
                    weather_data.append({
                        'chart_date': chart_date,
                        'weather_category': category,
                        'avg_sunshine': weather_avg.avg_sunshine or 0,
                        'avg_precipitation': weather_avg.avg_precipitation or 0,
                        'avg_temperature': weather_avg.avg_temperature or 0
                    })
        
        if not weather_data:
            return pd.DataFrame()
        
        weather_df = pd.DataFrame(weather_data)
        
        results = []
        
        for _, weather_row in weather_df.iterrows():
            chart_date = weather_row['chart_date']
            category = weather_row['weather_category']
            
            # Get all tracks from this chart
            tracks_query = self.db.query(
                DimTrack.track_id,
                DimTrack.danceability,
                DimTrack.energy,
                DimTrack.instrumentalness,
                DimTrack.loudness,
                DimTrack.valence,
                DimTrack.tempo,
                DimTrack.acousticness
            ).join(
                FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
            ).join(
                DimTime, FactTrackChart.date_id == DimTime.date_id
            ).filter(
                DimTime.date == chart_date,
                DimTrack.danceability.isnot(None)
            )
            
            if country:
                tracks_query = tracks_query.filter(FactTrackChart.country == country)
            
            tracks = tracks_query.all()
            
            for track in tracks:
                results.append({
                    'weather_category': category,
                    'avg_sunshine': weather_row['avg_sunshine'],
                    'avg_precipitation': weather_row['avg_precipitation'],
                    'avg_temperature': weather_row['avg_temperature'],
                    'track_id': track.track_id,
                    'danceability': track.danceability,
                    'energy': track.energy,
                    'instrumentalness': track.instrumentalness,
                    'loudness': track.loudness,
                    'valence': track.valence,
                    'tempo': track.tempo,
                    'acousticness': track.acousticness
                })
        
        if not results:
            return pd.DataFrame()
        
        # Aggregate by weather category
        results_df = pd.DataFrame(results)
        
        grouped = results_df.groupby('weather_category').agg({
            'avg_sunshine': 'mean',        
            'avg_precipitation': 'mean',   
            'avg_temperature': 'mean',     
            'danceability': 'mean',
            'energy': 'mean',
            'instrumentalness': 'mean',
            'loudness': 'mean',
            'valence': 'mean',
            'tempo': 'mean',
            'acousticness': 'mean',
            'track_id': 'count'  
        }).reset_index()
        
        # Rename and add metrics
        grouped.rename(columns={'track_id': 'chart_entries'}, inplace=True)
        
        # Count unique tracks per category
        unique_tracks = results_df.groupby('weather_category')['track_id'].nunique().reset_index()
        unique_tracks.rename(columns={'track_id': 'unique_tracks'}, inplace=True)
        
        # Merge
        final_df = grouped.merge(unique_tracks, on='weather_category')
        
        category_order = {'🌧️ Regnerisch (>5mm)': 0, '☀️ Sonnig (8-12h)': 1, '🌞 Sehr Sonnig (>12h)': 2}
        if not final_df.empty:
            final_df['sort_order'] = final_df['weather_category'].map(category_order)
            final_df = final_df.sort_values('sort_order').drop('sort_order', axis=1)
        
        return final_df
    
    def get_danceability_by_sunshine(self, country: str = 'de') -> pd.DataFrame:
        """Danceability & Valence korrelieren mit Sonnenstunden"""
        sunshine_category = case(
            (DimWeather.sunshine_hours == 0, '0h (Dunkel)'),
            (DimWeather.sunshine_hours < 4, '1-4h'),
            (DimWeather.sunshine_hours < 8, '4-8h'),
            (DimWeather.sunshine_hours < 12, '8-12h'),
            else_='12h+ (Sehr sonnig)'
        )
        
        query = self.db.query(
            sunshine_category.label('sunshine_hours'),
            func.avg(DimWeather.sunshine_hours).label('exact_sunshine'),
            func.avg(DimTrack.danceability).label('avg_danceability'),
            func.avg(DimTrack.valence).label('avg_valence')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimWeather, FactTrackChart.weather_id == DimWeather.weather_id
        ).filter(
            DimTrack.danceability.isnot(None),
            DimTrack.valence.isnot(None)  
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(
            sunshine_category
        ).having(
            func.count(FactTrackChart.fact_id) > 20
        )
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_seasonal_streaming_trends(self, country: str = None) -> pd.DataFrame:
        """Seasonal Trends"""
        query = self.db.query(
            DimTime.season,
            DimTime.month,
            func.sum(FactTrackChart.stream_count).label('total_streams')
        ).join(
            FactTrackChart, DimTime.date_id == FactTrackChart.date_id
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(
            DimTime.season, DimTime.month
        ).order_by(
            DimTime.month
        )
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_acoustic_vs_electronic(self, country: str = None) -> pd.DataFrame:
        """Akustisch vs Elektronisch nach Wetter"""
        acoustic_category = case(
            (DimTrack.acousticness > 0.7, 'Akustisch'),
            (DimTrack.acousticness < 0.3, 'Elektronisch'),
            else_='Hybrid'
        )
        
        weather_condition = case(
            (DimWeather.precipitation_mm > 5, 'Regen'),
            (DimWeather.sunshine_hours > 8, 'Sonne')
        )
        
        query = self.db.query(
            acoustic_category.label('track_type'),
            weather_condition.label('weather'),
            func.avg(FactTrackChart.stream_count).label('avg_streams'),
            func.count(FactTrackChart.fact_id).label('sample_size')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimWeather, FactTrackChart.weather_id == DimWeather.weather_id
        ).filter(
            DimTrack.acousticness.isnot(None)
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(
            acoustic_category, weather_condition
        ).having(
            func.count(FactTrackChart.fact_id) > 10
        )
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_key_distribution(self, country: str = None) -> pd.DataFrame:
        """Tonarten-Verteilung nach Jahreszeit"""
        query = self.db.query(
            DimTrack.key,
            DimTrack.mode,
            DimTime.season,
            func.count(FactTrackChart.fact_id).label('track_count')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            DimTrack.key.isnot(None),
            DimTrack.mode.isnot(None),
            DimTrack.key.between(0, 11)
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(DimTrack.key, DimTrack.mode, DimTime.season)
        
        df = pd.read_sql(query.statement, self.db.bind)
        
        # Build key_name in Python
        key_map = {
            0: 'C', 1: 'C#', 2: 'D', 3: 'D#', 
            4: 'E', 5: 'F', 6: 'F#', 7: 'G',
            8: 'G#', 9: 'A', 10: 'A#', 11: 'B'
        }
        
        mode_map = {0: 'm', 1: ''}
        
        df['key_name'] = df['key'].map(key_map) + df['mode'].map(mode_map).fillna('')
        
        result = df.groupby(['key_name', 'season'], as_index=False)['track_count'].sum()
        
        return result
    
    def get_lockdown_vs_normal_comparison(self, country: str = 'de') -> pd.DataFrame:
        """
        Lockdown-Phasen vs 2025 
        """
        from datetime import date
        
        # Define lockdown periods
        lockdown_periods = [
            (date(2020, 3, 22), date(2020, 5, 31)),   # Phase 1: Erster harter Lockdown
            (date(2020, 11, 2), date(2021, 5, 31)),   # Phase 2: Lockdown Light, danach verschärft
            (date(2021, 12, 2), date(2022, 3, 31))    # Phase 3: 2G-Plus
        ]
        
        # Normal year
        normal_start = date(2025, 1, 1)
        normal_end = date(2025, 12, 31)
        
        lockdown_conditions = [
            DimTime.date.between(start, end) for start, end in lockdown_periods
        ]
        
        # Lockdown data 
        lockdown_data = self.db.query(
            func.avg(DimTrack.valence).label('avg_valence'),
            func.avg(DimTrack.energy).label('avg_energy'),
            func.avg(DimTrack.danceability).label('avg_danceability'),
            func.count(FactTrackChart.fact_id).label('sample_size')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            or_(*lockdown_conditions),
            DimTrack.valence.isnot(None),
            DimTrack.energy.isnot(None),
            DimTrack.danceability.isnot(None)
        )
        
        if country:
            lockdown_data = lockdown_data.filter(FactTrackChart.country == country)
        
        lockdown_data = lockdown_data.first()
        
        # 2025 
        normal_data = self.db.query(
            func.avg(DimTrack.valence).label('avg_valence'),
            func.avg(DimTrack.energy).label('avg_energy'),
            func.avg(DimTrack.danceability).label('avg_danceability'),
            func.count(FactTrackChart.fact_id).label('sample_size')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            DimTime.date.between(normal_start, normal_end),
            DimTrack.valence.isnot(None),
            DimTrack.energy.isnot(None),
            DimTrack.danceability.isnot(None)
        )
        
        if country:
            normal_data = normal_data.filter(FactTrackChart.country == country)
        
        normal_data = normal_data.first()
        
        if not lockdown_data or not normal_data:
            return pd.DataFrame()
        
        # Calculate 
        diff = pd.DataFrame([
            {
                'feature': 'Valence',
                'lockdown': lockdown_data.avg_valence,
                'normal_2025': normal_data.avg_valence,
                'difference': normal_data.avg_valence - lockdown_data.avg_valence,
                'diff_pct': ((normal_data.avg_valence - lockdown_data.avg_valence) / lockdown_data.avg_valence) * 100
            },
            {
                'feature': 'Energy',
                'lockdown': lockdown_data.avg_energy,
                'normal_2025': normal_data.avg_energy,
                'difference': normal_data.avg_energy - lockdown_data.avg_energy,
                'diff_pct': ((normal_data.avg_energy - lockdown_data.avg_energy) / lockdown_data.avg_energy) * 100
            },
            {
                'feature': 'Danceability',
                'lockdown': lockdown_data.avg_danceability,
                'normal_2025': normal_data.avg_danceability,
                'difference': normal_data.avg_danceability - lockdown_data.avg_danceability,
                'diff_pct': ((normal_data.avg_danceability - lockdown_data.avg_danceability) / lockdown_data.avg_danceability) * 100
            }
        ])
        
        return diff
    
    def close(self):
        self.db.close()