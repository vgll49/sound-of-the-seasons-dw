# visualization/charts.py
import plotly.graph_objects as go
import plotly.express as px

def create_seasonal_chart(df):
    """Chart: Seasonal Streaming Trends"""
    fig = px.bar(
        df,
        x='month',
        y='total_streams',
        color='season',
        title='Streaming-Aktivität nach Monat und Jahreszeit',
        labels={
            'month': 'Monat',
            'total_streams': 'Gesamte Streams',
            'season': 'Jahreszeit'
        },
        color_discrete_map={
            'Winter': '#3b82f6',
            'Frühling': '#10b981',
            'Sommer': '#f59e0b',
            'Herbst': '#ef4444'
        }
    )
    fig.update_layout(
        height=450,
        xaxis=dict(tickmode='linear', tick0=1, dtick=1),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    return fig.to_html(include_plotlyjs='cdn', div_id='seasonal-chart', config={'displayModeBar': False})

def create_weather_chart(df):
    """Chart: Weather Impact"""
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['temperature_range'],
        y=df['total_streams'],
        marker_color='#8b5cf6',
        text=df['total_streams'].apply(lambda x: f'{int(x/1000)}K'),
        textposition='outside'
    ))
    
    fig.update_layout(
        title='Streaming nach Temperatur',
        xaxis_title='Temperaturbereich',
        yaxis_title='Streams',
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='weather-chart', config={'displayModeBar': False})

def create_mood_radar(df):
    """Chart: Mood by Weather"""
    fig = go.Figure()
    
    for _, row in df.iterrows():
        fig.add_trace(go.Scatterpolar(
            r=[row['avg_valence']*100, row['avg_energy']*100, row['avg_danceability']*100],
            theta=['Valence<br>(Positivität)', 'Energy<br>(Energie)', 'Danceability<br>(Tanzbarkeit)'],
            fill='toself',
            name=row['weather']
        ))
    
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        title='Track-Stimmung nach Wetterlage',
        height=500,
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='mood-chart', config={'displayModeBar': False})

def create_holiday_chart(df):
    """Chart: Holiday Effect (from DimTime.is_public_holiday)"""
    fig = px.pie(
        df,
        values='total_streams',
        names='day_type',
        title='Streaming: Feiertage vs Wochenende vs Werktage',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='holiday-chart', config={'displayModeBar': False})

def create_acoustic_chart(df):
    """Chart: Acoustic vs Electronic"""
    fig = px.bar(
        df,
        x='weather',
        y='avg_streams',
        color='track_type',
        barmode='group',
        title='Akustisch vs Elektronisch nach Wetter',
        labels={
            'weather': 'Wetter',
            'avg_streams': 'Ø Streams',
            'track_type': 'Track-Typ'
        },
        color_discrete_map={
            'Akustisch': '#10b981',
            'Elektronisch': '#f59e0b',
            'Hybrid': '#6b7280'
        }
    )
    
    fig.update_layout(
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='acoustic-chart', config={'displayModeBar': False})

def create_key_distribution_chart(df):
    """Chart: Key Distribution"""
    # Top 10 Keys
    top_keys = df.groupby('key_name')['track_count'].sum().nlargest(10).index
    df_filtered = df[df['key_name'].isin(top_keys)]
    
    fig = px.bar(
        df_filtered,
        x='key_name',
        y='track_count',
        color='season',
        title='Top 10 Tonarten nach Jahreszeit',
        labels={
            'key_name': 'Tonart',
            'track_count': 'Anzahl Tracks',
            'season': 'Jahreszeit'
        },
        color_discrete_map={
            'Winter': '#3b82f6',
            'Frühling': '#10b981',
            'Sommer': '#f59e0b',
            'Herbst': '#ef4444'
        }
    )
    
    fig.update_layout(
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='key-chart', config={'displayModeBar': False})

def create_tempo_weather_chart(df):
    """Tempo bei verschiedenen Wetterbedingungen"""
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['weather'],
        y=df['avg_tempo'],
        marker_color='#8b5cf6',
        text=df['avg_tempo'].apply(lambda x: f'{x:.0f} BPM'),
        textposition='outside',
        name='Tempo'
    ))
    
    fig.update_layout(
        title='Durchschnittliches Tempo nach Wetterlage',
        xaxis_title='Wetter',
        yaxis_title='Tempo (BPM)',
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='tempo-weather-chart', config={'displayModeBar': False})

def create_danceability_sunshine_chart(df):
    """Danceability vs Sonnenstunden"""
    # Sort by sunshine hours
    df = df.sort_values('exact_sunshine')
    
    fig = go.Figure()
    
    # Line chart
    fig.add_trace(go.Scatter(
        x=df['sunshine_hours'],
        y=df['avg_danceability'] * 100,
        mode='lines+markers',
        name='Danceability',
        line=dict(color='#f59e0b', width=3),
        marker=dict(size=10)
    ))
    
    # Valence on secondary axis
    fig.add_trace(go.Scatter(
        x=df['sunshine_hours'],
        y=df['avg_valence'] * 100,
        mode='lines+markers',
        name='Valence (Positivität)',
        line=dict(color='#10b981', width=3, dash='dash'),
        marker=dict(size=10),
        yaxis='y2'
    ))
    
    fig.update_layout(
        title='Danceability & Positivität nach Sonnenstunden',
        xaxis_title='Sonnenstunden',
        yaxis_title='Danceability (%)',
        yaxis2=dict(
            title='Valence (%)',
            overlaying='y',
            side='right'
        ),
        height=450,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif'),
        hovermode='x unified'
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='danceability-sunshine-chart', config={'displayModeBar': False})

def create_energy_temperature_scatter(df):
    """Scatter: Energy vs Temperature"""
    fig = px.scatter(
        df,
        x='temperature',
        y='avg_energy',
        color='season',
        size='sample_size',
        hover_data=['avg_tempo', 'sunshine'],
        title='Track Energy Level nach Temperatur',
        labels={
            'temperature': 'Temperatur (°C)',
            'avg_energy': 'Energy Level',
            'season': 'Jahreszeit'
        },
        color_discrete_map={
            'Winter': '#3b82f6',
            'Frühling': '#10b981',
            'Sommer': '#f59e0b',
            'Herbst': '#ef4444'
        }
    )
    
    fig.update_layout(
        height=450,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='energy-temp-scatter', config={'displayModeBar': False})

def create_winter_summer_comparison(df):
    """Winter vs Sommer - Feature Comparison Chart"""
    # Sortiere nach absoluter Differenz
    df = df.sort_values('diff_pct', key=abs, ascending=False)
    
    fig = go.Figure()
    
    # Winter values
    fig.add_trace(go.Bar(
        name='Winter ❄️',
        x=df['feature'],
        y=df['winter'],
        marker_color='#3b82f6',
        text=df['winter'].apply(lambda x: f'{x:.2f}'),
        textposition='outside'
    ))
    
    # Sommer values
    fig.add_trace(go.Bar(
        name='Sommer ☀️',
        x=df['feature'],
        y=df['sommer'],
        marker_color='#f59e0b',
        text=df['sommer'].apply(lambda x: f'{x:.2f}'),
        textposition='outside'
    ))
    
    fig.update_layout(
        title='Winter vs Sommer: Audio Feature Vergleich',
        xaxis_title='Audio Feature',
        yaxis_title='Durchschnittswert',
        barmode='group',
        height=450,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif'),
        showlegend=True
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='winter-summer-chart', config={'displayModeBar': False})

def create_seasonal_radar_all(df):
    """Radar Chart: Alle 4 Jahreszeiten"""
    # Normalize to 0-100 scale
    features = ['valence', 'energy', 'danceability', 'acousticness']
    
    fig = go.Figure()
    
    season_colors = {
        'Winter': '#3b82f6',
        'Frühling': '#10b981',
        'Sommer': '#f59e0b',
        'Herbst': '#ef4444'
    }
    
    for season in ['Winter', 'Frühling', 'Sommer', 'Herbst']:
        season_data = df[df['season'] == season]
        if not season_data.empty:
            row = season_data.iloc[0]
            
            fig.add_trace(go.Scatterpolar(
                r=[row[f] * 100 for f in features],
                theta=['Valence<br>(Positivität)', 'Energy<br>(Energie)', 
                       'Danceability<br>(Tanzbarkeit)', 'Acousticness<br>(Akustisch)'],
                fill='toself',
                name=season,
                line=dict(color=season_colors[season], width=2)
            ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100])
        ),
        title='Audio Feature Profile nach Jahreszeit',
        height=500,
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='seasonal-radar-all', config={'displayModeBar': False})
    
def create_loudness_rain_chart(df):
    """Loudness bei Regen vs Trocken"""
    fig = go.Figure()
    
    # Sort by precipitation
    df = df.sort_values('avg_precipitation')
    
    fig.add_trace(go.Bar(
        x=df['rain_category'],
        y=df['avg_loudness'],
        marker_color=['#3b82f6', '#60a5fa', '#93c5fd', '#dbeafe'],
        text=df['avg_loudness'].apply(lambda x: f'{x:.1f} dB'),
        textposition='outside'
    ))
    
    fig.update_layout(
        title='Track Loudness nach Niederschlag',
        xaxis_title='Niederschlagsmenge',
        yaxis_title='Loudness (dB)',
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='loudness-rain-chart', config={'displayModeBar': False})

def create_correlation_heatmap(df):
    """Korrelations-Heatmap: Wetter × Audio Features"""
    # Select only relevant correlations
    weather_cols = ['temperature', 'precipitation', 'sunshine', 'wind_speed']
    audio_cols = ['tempo', 'energy', 'danceability', 'valence', 'loudness', 'acousticness']
    
    # Extract cross-correlation
    correlation_matrix = df.loc[audio_cols, weather_cols]
    
    fig = go.Figure(data=go.Heatmap(
        z=correlation_matrix.values,
        x=['Temperatur', 'Regen', 'Sonne', 'Wind'],
        y=['Tempo', 'Energy', 'Danceability', 'Valence', 'Loudness', 'Acousticness'],
        colorscale='RdBu',
        zmid=0,
        text=correlation_matrix.values,
        texttemplate='%{text:.2f}',
        textfont={"size": 10},
        colorbar=dict(title="Korrelation")
    ))
    
    fig.update_layout(
        title='Korrelation: Wetter × Audio Features',
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif')
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='correlation-heatmap', config={'displayModeBar': False})

def create_top_tracks_table(df):
    """Table: Top Tracks per Season"""
    seasons_order = ['Frühling', 'Sommer', 'Herbst', 'Winter']
    icons = {
        'Frühling': '🌸',
        'Sommer': '☀️',
        'Herbst': '🍂',
        'Winter': '❄️'
    }
    
    html = '<div class="top-tracks-grid">'
    
    for season in seasons_order:
        season_data = df[df['season'] == season].head(5)
        
        if season_data.empty:
            continue
        
        html += f'''
        <div class="season-section">
            <h3>{icons[season]} {season}</h3>
            <div class="tracks-list">
        '''
        
        for _, row in season_data.iterrows():
            streams_formatted = f"{int(row['total_streams']/1000)}K"
            html += f'''
            <div class="track-item">
                <div class="track-info">
                    <div class="track-name">{row['track_name']}</div>
                    <div class="track-artist">{row['artist_names']}</div>
                </div>
                <div class="track-streams">{streams_formatted}</div>
            </div>
            '''
        
        html += '</div></div>'
    
    html += '</div>'
    return html