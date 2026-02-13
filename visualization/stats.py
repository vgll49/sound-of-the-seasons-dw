# visualization/stats.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import func, case
from sqlalchemy.orm import Session
from db.database import SessionLocal
from db.models import (
    FactTrackChart, DimTrack, DimTime, 
    DimWeather, DimHoliday
)
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class SoundOfSeasonsStats:
    def __init__(self):
        self.db: Session = SessionLocal()
    
    def get_kpis(self, country: str = None) -> dict:
        """KPIs - optional nach Country filtern"""
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
        ).filter(DimWeather.bundesland == "Berlin").scalar() or 0
        
        total_holidays = self.db.query(
            func.count(func.distinct(DimHoliday.date_id))
        ).scalar() or 0
        
        tracks_with_features = self.db.query(
            func.count(DimTrack.track_id)
        ).filter(DimTrack.danceability.isnot(None)).scalar() or 0
        
        return {
            'total_streams': f"{int(total_streams/1_000_000):.1f}M" if total_streams > 1_000_000 else f"{int(total_streams/1_000):.1f}K",
            'unique_tracks': unique_tracks,
            'country': country or 'All',
            'avg_temp': f"{avg_temp:.1f}",
            'total_holidays': total_holidays,
            'feature_coverage': f"{(tracks_with_features/unique_tracks*100):.0f}" if unique_tracks > 0 else "0"
        }

    def get_tempo_by_weather(self, country: str = 'de') -> pd.DataFrame:
        """Tempo bei Sonne vs Regen"""
        weather_condition = case(
            (DimWeather.precipitation_mm > 5, 'Regen ☔'),
            (DimWeather.sunshine_hours > 8, 'Sonne ☀️'),
            (DimWeather.sunshine_hours > 4, 'Teilweise Bewölkt 🌤️'),
            else_='Bewölkt ☁️'
        )
        
        query = self.db.query(
            weather_condition.label('weather'),
            func.avg(DimTrack.tempo).label('avg_tempo'),
            func.avg(DimTrack.energy).label('avg_energy'),
            func.avg(DimWeather.sunshine_hours).label('avg_sunshine'),
            func.count(FactTrackChart.fact_id).label('sample_size')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimWeather, FactTrackChart.weather_id == DimWeather.weather_id
        ).filter(
            DimTrack.tempo.isnot(None),
            DimWeather.bundesland == "Berlin"
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(
            weather_condition
        ).having(
            func.count(FactTrackChart.fact_id) > 20
        )
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_danceability_by_sunshine(self, country: str = 'de') -> pd.DataFrame:
        """Danceability korreliert mit Sonnenstunden"""
        # Gruppiere Sonnenstunden in Kategorien
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
            func.avg(DimTrack.valence).label('avg_valence'),
            func.avg(DimTrack.energy).label('avg_energy'),
            func.count(FactTrackChart.fact_id).label('sample_size')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimWeather, FactTrackChart.weather_id == DimWeather.weather_id
        ).filter(
            DimTrack.danceability.isnot(None),
            DimWeather.bundesland == "Berlin"
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(
            sunshine_category
        ).having(
            func.count(FactTrackChart.fact_id) > 20
        )
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_energy_by_temperature(self, country: str = 'de') -> pd.DataFrame:
        """Track Energy Level nach Temperatur - Scatter Plot Daten"""
        query = self.db.query(
            DimWeather.temperature_avg.label('temperature'),
            DimWeather.sunshine_hours.label('sunshine'),
            func.avg(DimTrack.energy).label('avg_energy'),
            func.avg(DimTrack.tempo).label('avg_tempo'),
            func.avg(DimTrack.danceability).label('avg_danceability'),
            DimTime.season,
            func.count(FactTrackChart.fact_id).label('sample_size')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimWeather, FactTrackChart.weather_id == DimWeather.weather_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            DimTrack.energy.isnot(None),
            DimWeather.bundesland == "Berlin"
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(
            DimWeather.temperature_avg,
            DimWeather.sunshine_hours,
            DimTime.season
        ).having(
            func.count(FactTrackChart.fact_id) > 5
        )
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_loudness_on_rainy_days(self, country: str = 'de') -> pd.DataFrame:
        """Loudness/Intensität an Regentagen vs Trockenen Tagen"""
        is_rainy = case(
            (DimWeather.precipitation_mm > 10, 'Stark Regnerisch (>10mm)'),
            (DimWeather.precipitation_mm > 5, 'Regnerisch (5-10mm)'),
            (DimWeather.precipitation_mm > 0, 'Leichter Regen (<5mm)'),
            else_='Trocken'
        )
        
        query = self.db.query(
            is_rainy.label('rain_category'),
            func.avg(DimWeather.precipitation_mm).label('avg_precipitation'),
            func.avg(DimTrack.loudness).label('avg_loudness'),
            func.avg(DimTrack.energy).label('avg_energy'),
            func.avg(DimTrack.valence).label('avg_valence'),
            func.count(FactTrackChart.fact_id).label('sample_size')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimWeather, FactTrackChart.weather_id == DimWeather.weather_id
        ).filter(
            DimTrack.loudness.isnot(None),
            DimWeather.bundesland == "Berlin"
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(
            is_rainy
        ).having(
            func.count(FactTrackChart.fact_id) > 20
        )
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_weather_audio_correlation(self, country: str = 'de') -> pd.DataFrame:
        """Korrelations-Matrix: Wetter × Audio Features"""
        query = self.db.query(
            DimWeather.temperature_avg.label('temperature'),
            DimWeather.precipitation_mm.label('precipitation'),
            DimWeather.sunshine_hours.label('sunshine'),
            DimWeather.wind_speed_kmh.label('wind_speed'),
            DimTrack.tempo,
            DimTrack.energy,
            DimTrack.danceability,
            DimTrack.valence,
            DimTrack.loudness,
            DimTrack.acousticness
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimWeather, FactTrackChart.weather_id == DimWeather.weather_id
        ).filter(
            DimTrack.tempo.isnot(None),
            DimWeather.bundesland == "Berlin"
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        df = pd.read_sql(query.statement, self.db.bind)
        
        # Berechne Korrelationen
        if len(df) > 0:
            correlation = df.corr()
            return correlation
        
        return pd.DataFrame()
    
    def get_seasonal_streaming_trends(self, country: str = None) -> pd.DataFrame:
        """Seasonal Trends - optional nach Country"""
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
    
    # NEU: Vergleich DE vs Global
    def get_country_comparison(self) -> pd.DataFrame:
        """Vergleich Deutschland vs Global Charts"""
        query = self.db.query(
            FactTrackChart.country,
            DimTime.season,
            func.avg(FactTrackChart.stream_count).label('avg_streams'),
            func.sum(FactTrackChart.stream_count).label('total_streams')
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).group_by(
            FactTrackChart.country, DimTime.season
        ).order_by(
            DimTime.season
        )
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_weather_impact(self, country: str = None) -> pd.DataFrame:
        """Analyse 2: Temperatur vs Streaming"""
        temp_category = case(
            (DimWeather.temperature_avg < 5, 'Kalt (< 5°C)'),
            (DimWeather.temperature_avg < 15, 'Kühl (5-15°C)'),
            (DimWeather.temperature_avg < 25, 'Mild (15-25°C)'),
            else_='Heiß (> 25°C)'
        )
        
        query = self.db.query(
            temp_category.label('temperature_range'),
            func.avg(FactTrackChart.stream_count).label('avg_streams'),
            func.sum(FactTrackChart.stream_count).label('total_streams')
        ).join(
            FactTrackChart, DimWeather.weather_id == FactTrackChart.weather_id
        ).filter(
            DimWeather.bundesland == "Berlin"
        )
        
        if country:  # NEU!
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(temp_category)
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_mood_by_weather(self, country: str = None) -> pd.DataFrame:
        """Analyse 3: Track Mood nach Wetterlage"""
        weather_condition = case(
            (DimWeather.precipitation_mm > 5, 'Regnerisch'),
            (DimWeather.sunshine_hours > 8, 'Sonnig'),
            (DimWeather.temperature_avg < 5, 'Kalt'),
            else_='Normal'
        )
        
        query = self.db.query(
            weather_condition.label('weather'),
            func.avg(DimTrack.valence).label('avg_valence'),
            func.avg(DimTrack.energy).label('avg_energy'),
            func.avg(DimTrack.danceability).label('avg_danceability'),
            func.count(FactTrackChart.fact_id).label('sample_size')  # NEU!
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimWeather, FactTrackChart.weather_id == DimWeather.weather_id
        ).filter(
            DimTrack.valence.isnot(None),
            DimTrack.energy.isnot(None),
            DimTrack.danceability.isnot(None),
            DimWeather.bundesland == "Berlin"
        )
        
        if country:  # NEU!
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(weather_condition).having(
            func.count(FactTrackChart.fact_id) > 10  # NEU: Min. sample
        )
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_holiday_effect(self, country: str = None) -> pd.DataFrame:
        """Analyse 4: Feiertage vs Normale Tage"""
        is_holiday = case(
            (DimHoliday.holiday_id.isnot(None), 'Feiertag'),
            (DimTime.is_weekend == True, 'Wochenende'),
            else_='Werktag'
        )
        
        query = self.db.query(
            is_holiday.label('day_type'),
            func.sum(FactTrackChart.stream_count).label('total_streams'),
            func.count(FactTrackChart.fact_id).label('chart_entries')  # NEU!
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).outerjoin(
            DimHoliday, FactTrackChart.holiday_id == DimHoliday.holiday_id
        )
        
        if country:  # NEU!
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(is_holiday)
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_top_tracks_by_season(self, country: str = None, limit: int = 5) -> pd.DataFrame:
        """Analyse 5: Top Tracks pro Jahreszeit"""
        query = self.db.query(
            DimTime.season,
            DimTrack.track_name,
            DimTrack.artist_names,
            func.sum(FactTrackChart.stream_count).label('total_streams')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        )
        
        if country:  # NEU!
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(
            DimTime.season, DimTrack.track_name, DimTrack.artist_names
        ).order_by(
            DimTime.season, func.sum(FactTrackChart.stream_count).desc()
        )
        
        df = pd.read_sql(query.statement, self.db.bind)
        result = df.groupby('season', group_keys=False).apply(
            lambda x: x.nlargest(limit, 'total_streams')
        ).reset_index(drop=True)
        
        return result
    
    def get_acoustic_vs_electronic(self, country: str = None) -> pd.DataFrame:
        """Analyse 6: Akustisch vs Elektronisch nach Wetter"""
        acoustic_category = case(
            (DimTrack.acousticness > 0.7, 'Akustisch'),
            (DimTrack.acousticness < 0.3, 'Elektronisch'),
            else_='Hybrid'
        )
        
        weather_condition = case(
            (DimWeather.precipitation_mm > 5, 'Regen'),
            (DimWeather.sunshine_hours > 8, 'Sonne'),
            else_='Normal'
        )
        
        query = self.db.query(
            acoustic_category.label('track_type'),
            weather_condition.label('weather'),
            func.avg(FactTrackChart.stream_count).label('avg_streams'),
            func.count(FactTrackChart.fact_id).label('sample_size')  # Renamed!
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimWeather, FactTrackChart.weather_id == DimWeather.weather_id
        ).filter(
            DimTrack.acousticness.isnot(None),
            DimWeather.bundesland == "Berlin"
        )
        
        if country:  # NEU!
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(
            acoustic_category, weather_condition
        ).having(
            func.count(FactTrackChart.fact_id) > 10  # NEU: Min. sample
        )
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_key_distribution(self, country: str = None) -> pd.DataFrame:
        """Analyse 7: Tonarten-Verteilung nach Jahreszeit"""
        key_mode = func.concat(
            case(
                (DimTrack.key == 0, 'C'),
                (DimTrack.key == 1, 'C#'),
                (DimTrack.key == 2, 'D'),
                (DimTrack.key == 3, 'D#'),
                (DimTrack.key == 4, 'E'),
                (DimTrack.key == 5, 'F'),
                (DimTrack.key == 6, 'F#'),
                (DimTrack.key == 7, 'G'),
                (DimTrack.key == 8, 'G#'),
                (DimTrack.key == 9, 'A'),
                (DimTrack.key == 10, 'A#'),
                (DimTrack.key == 11, 'B'),
                else_='?'
            ),
            case(
                (DimTrack.mode == 0, 'm'),
                (DimTrack.mode == 1, ''),
                else_=''
            )
        )
        
        query = self.db.query(
            key_mode.label('key_name'),
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
        
        if country:  # NEU!
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(key_mode, DimTime.season)
        
        return pd.read_sql(query.statement, self.db.bind)
    
    def get_winter_summer_comparison(self, country: str = 'de') -> pd.DataFrame:
        """Winter vs Sommer: Audio Feature Vergleich"""
        # Nur Winter und Sommer (klare Kontraste)
        season_filter = DimTime.season.in_(['Winter', 'Sommer'])
        
        query = self.db.query(
            DimTime.season,
            func.avg(DimTrack.valence).label('avg_valence'),
            func.avg(DimTrack.energy).label('avg_energy'),
            func.avg(DimTrack.tempo).label('avg_tempo'),
            func.avg(DimTrack.danceability).label('avg_danceability'),
            func.avg(DimTrack.acousticness).label('avg_acousticness'),
            func.avg(DimTrack.loudness).label('avg_loudness'),
            func.count(FactTrackChart.fact_id).label('sample_size')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            season_filter,
            DimTrack.valence.isnot(None),
            DimTrack.energy.isnot(None),
            DimTrack.tempo.isnot(None)
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(DimTime.season)
        
        df = pd.read_sql(query.statement, self.db.bind)
        
        # Berechne Differenzen
        if len(df) == 2:
            winter = df[df['season'] == 'Winter'].iloc[0]
            sommer = df[df['season'] == 'Sommer'].iloc[0]
            
            diff = pd.DataFrame([{
                'feature': 'Valence',
                'winter': winter['avg_valence'],
                'sommer': sommer['avg_valence'],
                'difference': sommer['avg_valence'] - winter['avg_valence'],
                'diff_pct': ((sommer['avg_valence'] - winter['avg_valence']) / winter['avg_valence']) * 100
            }, {
                'feature': 'Energy',
                'winter': winter['avg_energy'],
                'sommer': sommer['avg_energy'],
                'difference': sommer['avg_energy'] - winter['avg_energy'],
                'diff_pct': ((sommer['avg_energy'] - winter['avg_energy']) / winter['avg_energy']) * 100
            }, {
                'feature': 'Tempo',
                'winter': winter['avg_tempo'],
                'sommer': sommer['avg_tempo'],
                'difference': sommer['avg_tempo'] - winter['avg_tempo'],
                'diff_pct': ((sommer['avg_tempo'] - winter['avg_tempo']) / winter['avg_tempo']) * 100
            }, {
                'feature': 'Danceability',
                'winter': winter['avg_danceability'],
                'sommer': sommer['avg_danceability'],
                'difference': sommer['avg_danceability'] - winter['avg_danceability'],
                'diff_pct': ((sommer['avg_danceability'] - winter['avg_danceability']) / winter['avg_danceability']) * 100
            }, {
                'feature': 'Acousticness',
                'winter': winter['avg_acousticness'],
                'sommer': sommer['avg_acousticness'],
                'difference': sommer['avg_acousticness'] - winter['avg_acousticness'],
                'diff_pct': ((sommer['avg_acousticness'] - winter['avg_acousticness']) / winter['avg_acousticness']) * 100
            }, {
                'feature': 'Loudness',
                'winter': winter['avg_loudness'],
                'sommer': sommer['avg_loudness'],
                'difference': sommer['avg_loudness'] - winter['avg_loudness'],
                'diff_pct': ((sommer['avg_loudness'] - winter['avg_loudness']) / abs(winter['avg_loudness'])) * 100
            }])
            
            return diff
        
        return df

    def get_seasonal_audio_profile(self, country: str = 'de') -> pd.DataFrame:
        """Alle 4 Jahreszeiten - Audio Feature Profile"""
        query = self.db.query(
            DimTime.season,
            func.avg(DimTrack.valence).label('valence'),
            func.avg(DimTrack.energy).label('energy'),
            func.avg(DimTrack.tempo).label('tempo'),
            func.avg(DimTrack.danceability).label('danceability'),
            func.avg(DimTrack.acousticness).label('acousticness'),
            func.count(FactTrackChart.fact_id).label('sample_size')
        ).join(
            FactTrackChart, DimTrack.track_id == FactTrackChart.track_id
        ).join(
            DimTime, FactTrackChart.date_id == DimTime.date_id
        ).filter(
            DimTrack.valence.isnot(None)
        )
        
        if country:
            query = query.filter(FactTrackChart.country == country)
        
        query = query.group_by(DimTime.season)
        
        return pd.read_sql(query.statement, self.db.bind)

    def close(self):
        self.db.close()