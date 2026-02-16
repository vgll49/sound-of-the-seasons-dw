import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

def create_current_top_3(df):
    """ Top 3 with covers"""
    if df.empty:
        return "<p>Keine aktuellen Daten verfügbar</p>"
    
    html = '<div class="current-top-3">'
    
    for idx, row in df.iterrows():
        html += f'''
        <div class="top-track-card">
            <div class="rank-badge">#{int(row['position'])}</div>
            <img src="{row['image_url']}" alt="{row['track_name']}" class="track-cover">
            <div class="track-details">
                <h3 class="track-title">{row['track_name']}</h3>
                <p class="track-artist">{row['artist_names']}</p>
                <div class="track-stats">
                    <span class="stat-badge">{int(row['streams']/1000):.0f}K Streams</span>
                </div>
            </div>
        </div>
        '''
    
    html += '</div>'
    return html


def create_weekly_changes_widget(data):
    """Widget: Wöchentliche Audio Feature + Wetter Änderungen"""
    if not data or 'features' not in data:
        return "<p>Keine Daten verfügbar</p>"
    
    def get_arrow_and_color(change, threshold=0.005):
        """Return arrow emoji and color"""
        if abs(change) < threshold:
            return "→", "#6b7280", "gleich"
        elif change > 0:
            return "↑", "#10b981", "gestiegen"
        else:
            return "↓", "#ef4444", "gesunken"
    
    features_config = {
        'valence': {'name': 'Positivität', 'emoji': '😊', 'format': '{:.2f}'},
        'danceability': {'name': 'Tanzbarkeit', 'emoji': '💃', 'format': '{:.2f}'},
        'energy': {'name': 'Energie', 'emoji': '⚡', 'format': '{:.2f}'},
        'tempo': {'name': 'Tempo', 'emoji': '🎵', 'format': '{:.0f} BPM'},
        'acousticness': {'name': 'Akustisch', 'emoji': '🎸', 'format': '{:.2f}'}
    }
    
    weather_config = {
        'temperature': {'name': 'Temperatur', 'emoji': '🌡️', 'format': '{:.1f}°C', 'threshold': 0.5},
        'sunshine': {'name': 'Sonnenstunden', 'emoji': '☀️', 'format': '{:.1f}h', 'threshold': 0.5},
        'precipitation': {'name': 'Niederschlag', 'emoji': '💧', 'format': '{:.1f}mm', 'threshold': 0.5}
    }
    
    chart_date = data['chart_date'].strftime('%d.%m.%Y')
    prev_chart_date = data['previous_chart_date'].strftime('%d.%m.%Y')
    
    html = f'''
    <div class="weekly-changes-widget">
        <div class="widget-header">
            <h3>📈 Charts & Wetter im Vergleich</h3>
            <p class="comparison-dates">
                <span class="current-week">Charts {chart_date}</span>
                <span class="vs">vs</span>
                <span class="previous-week">{prev_chart_date}</span>
            </p>
            <p class="sample-info">
                {data['unique_songs']} unique Songs analysiert
            </p>
            <p class="weather-period-info">
                Wetter: {data['weather_period_current']} vs {data['weather_period_previous']}
            </p>
        </div>
        
        <!-- Audio Features -->
        <div class="section-label">🎵 Audio Features</div>
        <div class="changes-grid">
    '''
    
    # Audio Features
    for key, config in features_config.items():
        feat = data['features'][key]
        arrow, color, trend_text = get_arrow_and_color(feat['change'])
        
        current_val = config['format'].format(feat['current'])
        change_val = abs(feat['change'])
        change_pct = abs(feat['change_pct'])
        
        html += f'''
        <div class="change-card">
            <div class="feature-icon">{config['emoji']}</div>
            <div class="feature-name">{config['name']}</div>
            <div class="current-value">{current_val}</div>
            <div class="change-indicator" style="color: {color};">
                <span class="arrow">{arrow}</span>
                <span class="change-text">{trend_text}</span>
            </div>
            <div class="change-details">
                <span class="change-abs">{change_val:.3f}</span>
                <span class="change-pct">({change_pct:.1f}%)</span>
            </div>
        </div>
        '''
    
    html += '''
        </div>
        
        <!-- Weather -->
        <div class="section-label">🌤️ Wetter (Ø 7 Tage)</div>
        <div class="changes-grid weather-grid">
    '''
    
    # Weather
    for key, config in weather_config.items():
        weather = data['weather'][key]
        arrow, color, trend_text = get_arrow_and_color(
            weather['change'], 
            threshold=config['threshold']
        )
        
        current_val = config['format'].format(weather['current'])
        change_val = abs(weather['change'])
        
        html += f'''
        <div class="change-card weather-card">
            <div class="feature-icon">{config['emoji']}</div>
            <div class="feature-name">{config['name']}</div>
            <div class="current-value">{current_val}</div>
            <div class="change-indicator" style="color: {color};">
                <span class="arrow">{arrow}</span>
                <span class="change-text">{trend_text}</span>
            </div>
            <div class="change-details">
                <span class="change-abs">{change_val:.1f}</span>
            </div>
        </div>
        '''
    
    html += '''
        </div>
    </div>
    '''
    
    return html

def create_audio_features_timeline(df):
    """Line Chart: Audio Features über die Jahreszeiten"""
    if df.empty:
        return "<p>Keine Daten verfügbar</p>"
    
    # Season order and labels
    season_order = ['Frühling', 'Sommer', 'Herbst', 'Winter']
    season_emojis = {
        'Frühling': '🌸',
        'Sommer': '☀️', 
        'Herbst': '🍂',
        'Winter': '❄️'
    }
    
    # Ensure correct order
    df['season_order'] = df['season'].map({s: i for i, s in enumerate(season_order)})
    df = df.sort_values('season_order')
    
    # Create labels with emojis
    x_labels = [f"{season_emojis[s]} {s}" for s in df['season']]
    
    # Create hover text with both metrics
    hover_template = '<b>%{x}</b><br>%{fullData.name}: %{y:.3f}<br>' + \
                    'Chart Entries: %{customdata[0]:,}<br>' + \
                    'Tracks: %{customdata[1]:,}<extra></extra>'
    
    customdata = [[row['chart_entries'], row['unique_tracks']] for _, row in df.iterrows()]
    
    fig = go.Figure()
    
    # Danceability
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['danceability'],
        name='Danceability',
        mode='lines+markers',
        line=dict(color='#f59e0b', width=4),
        marker=dict(size=12),
        customdata=customdata,
        hovertemplate=hover_template
    ))
    
    # Energy
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['energy'],
        name='Energy',
        mode='lines+markers',
        line=dict(color='#ef4444', width=4),
        marker=dict(size=12),
        customdata=customdata,
        hovertemplate=hover_template
    ))
    
    # Instrumentalness
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['instrumentalness'],
        name='Instrumentalness',
        mode='lines+markers',
        line=dict(color='#8b5cf6', width=4),
        marker=dict(size=12),
        customdata=customdata,
        hovertemplate=hover_template
    ))
    
    # Valence
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['valence'],
        name='Valence',
        mode='lines+markers',
        line=dict(color='#10b981', width=4),
        marker=dict(size=12),
        customdata=customdata,
        hovertemplate=hover_template
    ))
    
    # Acousticness
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['acousticness'],
        name='Acousticness',
        mode='lines+markers',
        line=dict(color='#06b6d4', width=4),
        marker=dict(size=12),
        customdata=customdata,
        hovertemplate=hover_template
    ))
    
    # Season backgrounds
    season_colors = {
        'Frühling': 'rgba(16, 185, 129, 0.08)',
        'Sommer': 'rgba(245, 158, 11, 0.08)',
        'Herbst': 'rgba(239, 68, 68, 0.08)',
        'Winter': 'rgba(59, 130, 246, 0.08)'
    }
    
    # Add season background rectangles
    shapes = []
    for i, season in enumerate(df['season']):
        shapes.append(dict(
            type="rect",
            xref="x",
            yref="paper",
            x0=i - 0.4,
            x1=i + 0.4,
            y0=0,
            y1=1,
            fillcolor=season_colors.get(season, 'rgba(200,200,200,0.1)'),
            layer="below",
            line_width=0
        ))
    
    total_entries = int(df['chart_entries'].sum())
    total_unique = int(df['unique_tracks'].sum())
    
    fig.update_layout(
        title=f'Audio Features über die Jahreszeiten<br><sub>{total_entries:,} Chart-Einträge · {total_unique:,} Tracks</sub>',
        xaxis=dict(
            title='Jahreszeit'
        ),
        yaxis=dict(
            title='Wert (0-1)',
            range=[0, 1]
        ),
        height=450,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif'),
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        shapes=shapes
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='audio-features-timeline', config={'displayModeBar': False})


def create_tempo_timeline(df):
    """Line Chart: Tempo (BPM) über die Jahreszeiten"""
    if df.empty:
        return "<p>Keine Daten verfügbar</p>"
    
    # Season order and labels
    season_order = ['Frühling', 'Sommer', 'Herbst', 'Winter']
    season_emojis = {
        'Frühling': '🌸',
        'Sommer': '☀️',
        'Herbst': '🍂',
        'Winter': '❄️'
    }
    
    # Ensure correct order
    df['season_order'] = df['season'].map({s: i for i, s in enumerate(season_order)})
    df = df.sort_values('season_order')
    
    # Create labels with emojis
    x_labels = [f"{season_emojis[s]} {s}" for s in df['season']]
    
    # Season colors for markers
    season_colors_map = {
        'Winter': '#3b82f6',
        'Frühling': '#10b981',
        'Sommer': '#f59e0b',
        'Herbst': '#ef4444'
    }
    
    marker_colors = [season_colors_map.get(s, '#6b7280') for s in df['season']]
    
    # Create hover text
    hover_text = []
    for _, row in df.iterrows():
        hover_text.append(
            f"<b>{season_emojis[row['season']]} {row['season']}</b><br>" +
            f"Tempo: {row['tempo']:.1f} BPM<br>" +
            f"Chart Entries: {int(row['chart_entries']):,}<br>" +
            f"Tracks: {int(row['unique_tracks']):,}"
        )
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['tempo'],
        name='Tempo',
        mode='lines+markers',
        line=dict(color='#8b5cf6', width=5),
        marker=dict(
            size=16,
            color=marker_colors,
            line=dict(color='white', width=2)
        ),
        fill='tozeroy',
        fillcolor='rgba(139, 92, 246, 0.1)',
        hovertext=hover_text,
        hoverinfo='text'
    ))
    
    total_entries = int(df['chart_entries'].sum())
    total_unique = int(df['unique_tracks'].sum())
    
    fig.update_layout(
        title=f'Tempo (BPM) über die Jahreszeiten<br><sub>{total_entries:,} Chart-Einträge · {total_unique:,} Tracks</sub>',
        xaxis=dict(
            title='Jahreszeit'
        ),
        yaxis=dict(
            title='Tempo (BPM)',
            range=[df['tempo'].min() * 0.95, df['tempo'].max() * 1.05]
        ),
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif'),
        showlegend=False
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='tempo-timeline', config={'displayModeBar': False})


def create_loudness_timeline(df):
    """Bar Chart: Loudness (dB) über die Jahreszeiten - SEPARATER CHART"""
    if df.empty:
        return "<p>Keine Daten verfügbar</p>"
    
    # Season order and labels
    season_order = ['Frühling', 'Sommer', 'Herbst', 'Winter']
    season_emojis = {
        'Frühling': '🌸',
        'Sommer': '☀️',
        'Herbst': '🍂',
        'Winter': '❄️'
    }
    
    # Ensure correct order
    df['season_order'] = df['season'].map({s: i for i, s in enumerate(season_order)})
    df = df.sort_values('season_order')
    
    # Create labels with emojis
    x_labels = [f"{season_emojis[s]} {s}" for s in df['season']]
    
    # Season colors
    season_colors_map = {
        'Winter': '#3b82f6',
        'Frühling': '#10b981',
        'Sommer': '#f59e0b',
        'Herbst': '#ef4444'
    }
    
    bar_colors = [season_colors_map.get(s, '#6b7280') for s in df['season']]
    
    # Create hover text
    hover_text = []
    for _, row in df.iterrows():
        hover_text.append(
            f"<b>{season_emojis[row['season']]} {row['season']}</b><br>" +
            f"Loudness: {row['loudness']:.1f} dB<br>" +
            f"Chart Entries: {int(row['chart_entries']):,}<br>" +
            f"Tracks: {int(row['unique_tracks']):,}"
        )
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=x_labels,
        y=df['loudness'],
        marker=dict(
            color=bar_colors,
            line=dict(color='white', width=2)
        ),
        text=df['loudness'].apply(lambda x: f'{x:.1f} dB'),
        textposition='outside',
        hovertext=hover_text,
        hoverinfo='text'
    ))
    
    total_entries = int(df['chart_entries'].sum())
    total_unique = int(df['unique_tracks'].sum())
    
    fig.update_layout(
        title=f'Loudness (dB) über die Jahreszeiten<br><sub>{total_entries:,} Chart-Einträge · {total_unique:,} Tracks</sub>',
        xaxis=dict(
            title='Jahreszeit'
        ),
        yaxis=dict(
            title='Loudness (dB)',
            range=[df['loudness'].min() * 1.1, df['loudness'].max() * 0.9]  # Inverted because dB is negative
        ),
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif'),
        showlegend=False
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='loudness-timeline', config={'displayModeBar': False})


def create_audio_features_by_weather(df):
    """Line Chart: Audio Features über Wetter-Kategorien (Regen + Sonne)"""
    if df.empty:
        return "<p>Keine Daten verfügbar</p>"
    
    # Already sorted in the query (rain first, then by sunshine)
    x_labels = df['weather_category'].tolist()
    
    # Create hover text with weather metrics
    hover_template = '<b>%{x}</b><br>%{fullData.name}: %{y:.3f}<br>' + \
                    'Ø Sonne: %{customdata[0]:.1f}h<br>' + \
                    'Ø Regen: %{customdata[1]:.1f}mm<br>' + \
                    'Ø Temp: %{customdata[2]:.1f}°C<br>' + \
                    'Chart Entries: %{customdata[3]:,}<br>' + \
                    'Tracks: %{customdata[4]:,}<extra></extra>'
    
    customdata = [[row['avg_sunshine'], row['avg_precipitation'], row['avg_temperature'],
                   row['chart_entries'], row['unique_tracks']] for _, row in df.iterrows()]
    
    fig = go.Figure()
    
    # Danceability
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['danceability'],
        name='Danceability',
        mode='lines+markers',
        line=dict(color='#f59e0b', width=4),
        marker=dict(size=12),
        customdata=customdata,
        hovertemplate=hover_template
    ))
    
    # Energy
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['energy'],
        name='Energy',
        mode='lines+markers',
        line=dict(color='#ef4444', width=4),
        marker=dict(size=12),
        customdata=customdata,
        hovertemplate=hover_template
    ))
    
    # Valence
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['valence'],
        name='Valence',
        mode='lines+markers',
        line=dict(color='#10b981', width=4),
        marker=dict(size=12),
        customdata=customdata,
        hovertemplate=hover_template
    ))
    
    # Instrumentalness
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['instrumentalness'],
        name='Instrumentalness',
        mode='lines+markers',
        line=dict(color='#8b5cf6', width=4),
        marker=dict(size=12),
        customdata=customdata,
        hovertemplate=hover_template
    ))
    
    # Acousticness
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['acousticness'],
        name='Acousticness',
        mode='lines+markers',
        line=dict(color='#06b6d4', width=4),
        marker=dict(size=12),
        customdata=customdata,
        hovertemplate=hover_template
    ))
    
    total_entries = int(df['chart_entries'].sum())
    total_unique = int(df['unique_tracks'].sum())
    
    fig.update_layout(
        title=f'Audio Features nach Wetter-Bedingungen (Regen + Sonne)<br><sub>{total_entries:,} Chart-Einträge · {total_unique:,} Tracks</sub>',
        xaxis=dict(
            title='Wetter-Bedingungen',
            tickangle=-15
        ),
        yaxis=dict(
            title='Wert (0-1)',
            range=[0, 1]
        ),
        height=450,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif'),
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='audio-features-weather', config={'displayModeBar': False})


def create_tempo_by_weather(df):
    """Line Chart: Tempo über Wetter-Kategorien (Regen + Sonne)"""
    if df.empty:
        return "<p>Keine Daten verfügbar</p>"
    
    x_labels = df['weather_category'].tolist()
    
    marker_colors = []
    for _, row in df.iterrows():
        if row['avg_precipitation'] > 5:
            marker_colors.append('#3b82f6')  
        elif row['avg_precipitation'] > 1:
            marker_colors.append('#60a5fa')  
        elif row['avg_sunshine'] < 4:
            marker_colors.append('#94a3b8')  
        elif row['avg_sunshine'] < 8:
            marker_colors.append('#fbbf24') 
        else:
            marker_colors.append('#fcd34d')  
    
    # Create hover text
    hover_text = []
    for _, row in df.iterrows():
        hover_text.append(
            f"<b>{row['weather_category']}</b><br>" +
            f"Tempo: {row['tempo']:.1f} BPM<br>" +
            f"Ø Sonne: {row['avg_sunshine']:.1f}h<br>" +
            f"Ø Regen: {row['avg_precipitation']:.1f}mm<br>" +
            f"Ø Temp: {row['avg_temperature']:.1f}°C<br>" +
            f"Chart Entries: {int(row['chart_entries']):,}<br>" +
            f"Tracks: {int(row['unique_tracks']):,}"
        )
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=x_labels,
        y=df['tempo'],
        name='Tempo',
        mode='lines+markers',
        line=dict(color='#8b5cf6', width=5),
        marker=dict(
            size=16,
            color=marker_colors,
            line=dict(color='white', width=2)
        ),
        fill='tozeroy',
        fillcolor='rgba(139, 92, 246, 0.1)',
        hovertext=hover_text,
        hoverinfo='text'
    ))
    
    total_entries = int(df['chart_entries'].sum())
    total_unique = int(df['unique_tracks'].sum())
    
    fig.update_layout(
        title=f'Tempo (BPM) nach Wetter-Bedingungen (Regen + Sonne)<br><sub>{total_entries:,} Chart-Einträge · {total_unique:,}Tracks</sub>',
        xaxis=dict(
            title='Wetter-Bedingungen',
            tickangle=-15
        ),
        yaxis=dict(
            title='Tempo (BPM)',
            range=[df['tempo'].min() * 0.95, df['tempo'].max() * 1.05]
        ),
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif'),
        showlegend=False
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='tempo-weather', config={'displayModeBar': False})


def create_loudness_by_weather(df):
    """Bar Chart: Loudness über Wetter-Kategorien (Regen + Sonne)"""
    if df.empty:
        return "<p>Keine Daten verfügbar</p>"
    
    x_labels = df['weather_category'].tolist()
    
    # Create gradient colors from rain (blue) to sun (yellow)
    bar_colors = []
    for _, row in df.iterrows():
        if row['avg_precipitation'] > 5:
            bar_colors.append('#3b82f6')  # Blue for rain
        elif row['avg_precipitation'] > 1:
            bar_colors.append('#60a5fa')  # Light blue
        elif row['avg_sunshine'] < 4:
            bar_colors.append('#94a3b8')  # Gray
        elif row['avg_sunshine'] < 8:
            bar_colors.append('#fbbf24')  # Yellow
        else:
            bar_colors.append('#fcd34d')  # Bright yellow
    
    # Create hover text
    hover_text = []
    for _, row in df.iterrows():
        hover_text.append(
            f"<b>{row['weather_category']}</b><br>" +
            f"Loudness: {row['loudness']:.1f} dB<br>" +
            f"Ø Sonne: {row['avg_sunshine']:.1f}h<br>" +
            f"Ø Regen: {row['avg_precipitation']:.1f}mm<br>" +
            f"Ø Temp: {row['avg_temperature']:.1f}°C<br>" +
            f"Chart Entries: {int(row['chart_entries']):,}<br>" +
            f"Tracks: {int(row['unique_tracks']):,}"
        )
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=x_labels,
        y=df['loudness'],
        marker=dict(
            color=bar_colors,
            line=dict(color='white', width=2)
        ),
        text=df['loudness'].apply(lambda x: f'{x:.1f} dB'),
        textposition='outside',
        hovertext=hover_text,
        hoverinfo='text'
    ))
    
    total_entries = int(df['chart_entries'].sum())
    total_unique = int(df['unique_tracks'].sum())
    
    fig.update_layout(
        title=f'Loudness (dB) nach Wetter-Bedingungen (Regen + Sonne)<br><sub>{total_entries:,} Chart-Einträge · {total_unique:,}Tracks</sub>',
        xaxis=dict(
            title='Wetter-Bedingungen',
            tickangle=-15
        ),
        yaxis=dict(
            title='Loudness (dB)',
            range=[df['loudness'].min() * 1.1, df['loudness'].max() * 0.9]
        ),
        height=400,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif'),
        showlegend=False
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='loudness-weather', config={'displayModeBar': False})


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
    
def create_lockdown_vs_normal_comparison(df):
    """Lockdown vs 2025 - Feature Comparison Chart"""
    if df.empty:
        return "<p>Keine Daten verfügbar</p>"
    
    # Sortiere nach absoluter Differenz
    df = df.sort_values('diff_pct', key=abs, ascending=False)
    
    fig = go.Figure()
    
    # Lockdown values
    fig.add_trace(go.Bar(
        name='Lockdowns',
        x=df['feature'],
        y=df['lockdown'],
        marker_color='#ef4444',
        text=df['lockdown'].apply(lambda x: f'{x:.2f}'),
        textposition='outside'
    ))
    
    # 2025 values
    fig.add_trace(go.Bar(
        name='2025',
        x=df['feature'],
        y=df['normal_2025'],
        marker_color='#10b981',
        text=df['normal_2025'].apply(lambda x: f'{x:.2f}'),
        textposition='outside'
    ))
    
    fig.update_layout(
        title='Lockdown vs 2025: Wie COVID unsere Musik veränderte<br><sub>März 2020-Mai 2021, Dez 2021-März 2022 vs Jan-Dez 2025</sub>',
        xaxis_title='Audio Feature',
        yaxis_title='Durchschnittswert',
        barmode='group',
        height=450,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(family='Inter, system-ui, sans-serif'),
        showlegend=True
    )
    
    return fig.to_html(include_plotlyjs=False, div_id='lockdown-comparison-chart', config={'displayModeBar': False})