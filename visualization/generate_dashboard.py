# visualization/generate_dashboard.py - CLEAN!
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jinja2 import Template
from stats import SoundOfSeasonsStats
import charts
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_dashboard(country: str = 'de'):
    logger.info("Starting dashboard generation...")
    
    stats = SoundOfSeasonsStats()
    
    # Existing data
    kpis = stats.get_kpis(country=country)
    seasonal_df = stats.get_seasonal_streaming_trends(country=country)
    weather_df = stats.get_weather_impact(country=country)
    mood_df = stats.get_mood_by_weather(country=country)
    top_tracks_df = stats.get_top_tracks_by_season(country=country, limit=5)
    acoustic_df = stats.get_acoustic_vs_electronic(country=country)
    key_df = stats.get_key_distribution(country=country)
    
    # NEW: Weather × Audio Feature Analysen
    tempo_weather_df = stats.get_tempo_by_weather(country=country)
    danceability_sun_df = stats.get_danceability_by_sunshine(country=country)
    energy_temp_df = stats.get_energy_by_temperature(country=country)
    loudness_rain_df = stats.get_loudness_on_rainy_days(country=country)
    correlation_df = stats.get_weather_audio_correlation(country=country)
    winter_summer_df = stats.get_winter_summer_comparison(country=country)
    seasonal_profile_df = stats.get_seasonal_audio_profile(country=country)

    
    stats.close()
    
    # Create charts
    logger.info("Creating charts...")
    chart_html = {
        # Existing
        'seasonal': charts.create_seasonal_chart(seasonal_df),
        'weather': charts.create_weather_chart(weather_df),
        'mood': charts.create_mood_radar(mood_df),
        'acoustic': charts.create_acoustic_chart(acoustic_df),
        'key_distribution': charts.create_key_distribution_chart(key_df),
        'top_tracks': charts.create_top_tracks_table(top_tracks_df),

        
        # NEW: Weather × Audio Features
        'tempo_weather': charts.create_tempo_weather_chart(tempo_weather_df),
        'danceability_sunshine': charts.create_danceability_sunshine_chart(danceability_sun_df),
        'energy_temperature': charts.create_energy_temperature_scatter(energy_temp_df),
        'loudness_rain': charts.create_loudness_rain_chart(loudness_rain_df),
        'correlation': charts.create_correlation_heatmap(correlation_df),
        'winter_summer': charts.create_winter_summer_comparison(winter_summer_df),
        'seasonal_radar': charts.create_seasonal_radar_all(seasonal_profile_df)
    }
    
    # 3. Load Template
    logger.info("Rendering template...")
    template_path = os.path.join(os.path.dirname(__file__), 'templates', 'dashboard.html')
    
    with open(template_path, 'r', encoding='utf-8') as f:
        template = Template(f.read())
    
    # 4. Render HTML
    html = template.render(
        kpis=kpis,
        charts=chart_html
    )
    
    # 5. Save Output
    output_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        'docs',
        'index.html'
    )
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    logger.info(f"✅ Dashboard generated: {output_path}")
    logger.info("\n🚀 Next steps:")
    logger.info("   1. Test locally: python -m http.server --directory docs 8000")
    logger.info("   2. Open: http://localhost:8000")

if __name__ == "__main__":
    generate_dashboard()