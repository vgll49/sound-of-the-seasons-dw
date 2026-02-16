import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from jinja2 import Template
from visualization.stats import SoundOfSeasonsStats
import visualization.charts as charts
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_dashboard(country: str = 'de'):
    logger.info("Starting dashboard generation...")
    
    stats = SoundOfSeasonsStats()
    
    # KPIs & Current Week
    kpis = stats.get_kpis(country=country)
    current_top_3_df = stats.get_current_top_tracks(country=country, limit=3)
    weekly_changes = stats.get_weekly_feature_changes(country=country)
    
    # Seasonal Analysis
    audio_by_season_df = stats.get_audio_features_by_season(country=country)
    seasonal_df = stats.get_seasonal_streaming_trends(country=country)
    
    # Weather Analysis
    audio_by_weather_df = stats.get_audio_features_by_weather(country=country)
    danceability_sun_df = stats.get_danceability_by_sunshine(country=country)
    
    # Music Analysis
    acoustic_df = stats.get_acoustic_vs_electronic(country=country)
    key_df = stats.get_key_distribution(country=country)
    
    # COVID Comparison
    lockdown_df = stats.get_lockdown_vs_normal_comparison(country=country)
    
    stats.close()
    
    logger.info("Creating charts...")
    
    chart_html = {
        # KPIs & Widgets
        'current_top_3': charts.create_current_top_3(current_top_3_df),
        'weekly_changes': charts.create_weekly_changes_widget(weekly_changes),
        
        # Seasonal Charts
        'audio_features_timeline': charts.create_audio_features_timeline(audio_by_season_df),
        'tempo_timeline': charts.create_tempo_timeline(audio_by_season_df),
        'loudness_timeline': charts.create_loudness_timeline(audio_by_season_df),
        'seasonal': charts.create_seasonal_chart(seasonal_df),
        
        # Weather Charts
        'audio_features_weather': charts.create_audio_features_by_weather(audio_by_weather_df),
        'tempo_weather_new': charts.create_tempo_by_weather(audio_by_weather_df),
        'loudness_weather_new': charts.create_loudness_by_weather(audio_by_weather_df),
        'danceability_sunshine': charts.create_danceability_sunshine_chart(danceability_sun_df),
        
        # Music Analysis Charts
        'acoustic': charts.create_acoustic_chart(acoustic_df),
        'key_distribution': charts.create_key_distribution_chart(key_df),
        
        # COVID Comparison
        'lockdown_comparison': charts.create_lockdown_vs_normal_comparison(lockdown_df),
    }
    
    # Render Template
    logger.info("Rendering template...")
    template_path = Path(__file__).parent / 'templates' / 'dashboard.html'
    
    with open(template_path, 'r', encoding='utf-8') as f:
        template = Template(f.read())
    
    html = template.render(
        kpis=kpis,
        charts=chart_html
    )
    
    # Save Output
    output_dir = project_root / 'docs'
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / 'index.html'
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    logger.info(f"Dashboard generated: {output_path}")
    logger.info("\nNext steps:")
    logger.info("   1. Test locally: python -m http.server --directory docs 8000")


if __name__ == "__main__":
    generate_dashboard()