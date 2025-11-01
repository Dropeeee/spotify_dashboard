from flask import Flask, render_template, jsonify, request, session, redirect, url_for
import os
import spotipy
from werkzeug.utils import secure_filename
import uuid
import shutil
import glob
from spotipy.oauth2 import SpotifyOAuth
import spotipy.util as util
from datetime import datetime, timedelta
import pandas as pd
import json
from spotify_api import SpotifyEnhancer
from data_processing import (
    load_streaming_history, 
    filter_music,
    top_tracks,
    top_artists, 
    top_albums,
    daily_history,
    repeat_spirals_optimized,
    viciado_tracks_top20,
    set_spotify_enhancer,
    enrich_with_spotify_metadata_fast
)
from config import Config

spotify_enhancer_instance = None

def init_spotify_enhancer():
    """Inicializa SpotifyEnhancer com credenciais do Config"""
    global spotify_enhancer_instance
    if spotify_enhancer_instance is None:
        spotify_enhancer_instance = SpotifyEnhancer(
            client_id=Config.SPOTIFY_CLIENT_ID,
            client_secret=Config.SPOTIFY_CLIENT_SECRET
        )
        # Passar instância para data_processing
        set_spotify_enhancer(spotify_enhancer_instance)
        print(f"✅ SpotifyEnhancer inicializado (api_available={spotify_enhancer_instance.api_available})")
    return spotify_enhancer_instance

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = Config.SECRET_KEY

with app.app_context():
    init_spotify_enhancer()

# Ã¢Å“â€¦ ADICIONAR configuraÃƒÂ§ÃƒÂ£o de sessÃƒÂ£o:
from datetime import timedelta

# ConfiguraÃƒÂ§ÃƒÂ£o baseada no ambiente
if os.environ.get('FLASK_ENV') == 'production':
    app.config['SESSION_COOKIE_SECURE'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'None'
else:
    app.config['SESSION_COOKIE_SECURE'] = False
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

app.config['SESSION_COOKIE_NAME'] = 'spotify_dashboard_session'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)



@app.before_request
def setup_session():
    """Setup session permanente"""
    session.permanent = True
    # NÃƒÆ’O criar user_id aqui! Criar sÃƒÂ³ em /api/save-username



# Criar pasta de uploads
os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)


# Global cache for performance
app_cache = {}

# ============================================================================
# FUNÃƒâ€¡Ãƒâ€¢ES MULTI-USER
# ============================================================================


def allowed_file(filename):
    """Valida se ficheiro ÃƒÂ© .json"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'json'

def get_user_folder():
    """Retorna pasta ÃƒÂºnica por utilizador"""
    if 'user_id' not in session:
        raise ValueError("Ã¢ÂÅ’ user_id nÃƒÂ£o existe na sessÃƒÂ£o!")
    user_folder = os.path.join(Config.UPLOAD_FOLDER, session['user_id'])
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def load_user_data_from_files(user_folder):
    """Carrega dados de ficheiros JSON de uma pasta especÃƒÂ­fica"""
    json_files = [f for f in os.listdir(user_folder) if f.endswith('.json')]
    
    if not json_files:
        raise FileNotFoundError(f"No JSON files in {user_folder}")
    
    print(f"Ã°Å¸â€œÅ  Loading {len(json_files)} JSON files from user folder...")
    all_data = []
    
    for json_file in json_files:
        filepath = os.path.join(user_folder, json_file)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
        except Exception as e:
            print(f"Ã¢ÂÅ’ Error loading {json_file}: {e}")
            continue
    
    if not all_data:
        raise ValueError("No data loaded from JSON files")
    
    # Converter para DataFrame (mesma estrutura que load_streaming_history())
    df = pd.DataFrame(all_data)
    df['ts'] = pd.to_datetime(df['ts'])
    if df['ts'].dt.tz is not None:
        df['ts'] = df['ts'].dt.tz_convert(None)
    
    print(f"Ã¢Å“â€¦ {len(df):,} records loaded from user files")
    return df



def get_spotify_client():
    """Spotify client with all required scopes"""
    scope = 'user-top-read playlist-modify-public playlist-modify-private streaming user-read-private user-modify-playback-state user-read-playback-state'
    
    if 'user_id' in session and session.get('files_uploaded'):
        cache_path = os.path.join(get_user_folder(), '.spotify_cache')
    else:
        cache_path = '.cache-default'

    try:
        redirect_uri = Config.REDIRECT_URI  # Ã¢Å“â€¦ ADICIONA ESTA LINHA

        auth_manager = SpotifyOAuth(
        client_id=Config.SPOTIFY_CLIENT_ID,
        client_secret=Config.SPOTIFY_CLIENT_SECRET,
        redirect_uri=redirect_uri,
        scope=scope,  # ✅ ADICIONAR esta linha
        cache_path=cache_path  # ✅ ADICIONAR esta linha
    )


        
        token_info = auth_manager.get_cached_token()
        if not token_info:
            return None
        
        if auth_manager.is_token_expired(token_info):
            token_info = auth_manager.refresh_access_token(token_info['refresh_token'])
        
        return spotipy.Spotify(auth=token_info['access_token'])
        
    except Exception as e:
        print(f"Ã¢ÂÅ’ Authentication error: {e}")
        return None
    
def load_local_data():
    """Load data ISOLADO por utilizador OU de path local"""
    if 'user_id' in session:
        try:
            user_folder = get_user_folder()
            
            # ✅ ADICIONAR DEBUG
            print(f"\n{'='*70}")
            print(f"LOAD_LOCAL_DATA - User: {session['user_id'][:8]}...")
            print(f"User folder: {user_folder}")
            print(f"Folder exists: {os.path.exists(user_folder)}")
            
            if os.path.exists(user_folder):
                all_files = os.listdir(user_folder)
                json_files = [f for f in all_files if f.endswith('.json')]
                print(f"All files: {all_files}")
                print(f"JSON files: {json_files}")
            print(f"{'='*70}\n")
            
            if json_files:
                # ✅ Utilizador tem dados! Processar
                print(f"📁 User {session['user_id'][:8]} has {len(json_files)} files")
                session['files_uploaded'] = True  # ✅ Atualizar sessão
                
                cache_key = f'df_music_{session["user_id"]}'
                if cache_key not in app_cache:
                    try:
                        # Verificar cache em disco
                        cache_file = os.path.join(user_folder, 'processed_data.pkl')
                        
                        if os.path.exists(cache_file):
                            print(f"📁 Loading cached data for user {session['user_id'][:8]}...")
                            # ✅ ADICIONAR debug
                            print(f"📁 Cache file: {cache_file}")
                            print(f"📁 File size: {os.path.getsize(cache_file):,} bytes")
                            print(f"📁 File modified: {datetime.fromtimestamp(os.path.getmtime(cache_file))}")
                            
                            df_music = pd.read_pickle(cache_file)
                            app_cache[cache_key] = df_music
                            
                            if not df_music.empty:
                                session['data_loaded'] = True
                                session.modified = True
                                print("✅ Session marked: data_loaded = True")
                            
                            print(f"✅ Loaded {len(df_music):,} records from cache")
                            return df_music
                        
                        # Processar ficheiros uploaded
                        print(f"📁 Processing uploaded files for user {session['user_id'][:8]}...")
                        df = load_user_data_from_files(user_folder)
                        df_music = filter_music(df)
                        
                        # Guardar cache
                        df_music.to_pickle(cache_file)
                        app_cache[cache_key] = df_music
                        session['data_loaded'] = True
                        
                        print(f"✅ {len(df_music):,} records processed from uploaded files")
                        return df_music
                    
                    except Exception as e:
                        print(f"❌ Error loading user data: {e}")
                        import traceback
                        traceback.print_exc()
                        app_cache[cache_key] = pd.DataFrame()
                
                return app_cache.get(cache_key, pd.DataFrame())
            
            else:
                # ❌ User tem user_id mas NÃO tem ficheiros
                print(f"⚠️ User {session['user_id'][:8]} has NO files")
                return pd.DataFrame()
        
        except Exception as e:
            print(f"❌ Error in load_local_data: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    # Development mode (path hardcoded)
    else:
        if 'df_music_default' not in app_cache:
            try:
                print("📁 Loading from hardcoded path (development mode)")
                df = load_streaming_history()
                df_music = filter_music(df)
                app_cache['df_music_default'] = df_music
                print(f"✅ {len(df_music):,} records loaded")
            except Exception as e:
                print(f"❌ Error: {e}")
                app_cache['df_music_default'] = pd.DataFrame()
        
        return app_cache['df_music_default']



def get_top_tracks_api_with_images(time_range, limit=50):
    """Get top tracks from Spotify API with images and IDs"""
    sp = get_spotify_client()
    if sp:
        try:
            results = sp.current_user_top_tracks(limit=limit, time_range=time_range)
            
            tracks = []
            for track in results['items']:
                # Get best available image
                image_url = None
                if track['album']['images']:
                    images = track['album']['images']
                    image_url = images[1]['url'] if len(images) > 1 else images[0]['url']
                
                tracks.append({
                    'id': track['id'],
                    'name': track['name'],
                    'uri': track['uri'],
                    'artist': track['artists'][0]['name'],
                    'album': track['album']['name'],
                    'image_url': image_url,
                    'spotify_url': track['external_urls']['spotify'],
                    'popularity': track['popularity'],
                    'preview_url': track['preview_url'],
                    'duration_ms': track['duration_ms'],
                    'track_id': track['id']
                })
            
            return tracks
        except Exception as e:
            print(f"Ã¢ÂÅ’ API error: {e}")
            return []
    return []

def search_track_get_id(track_name, artist_name):
    sp = get_spotify_client()
    if not sp:
        print(f"Ã¢ÂÅ’ No Spotify client available")
        return None
    
    try:
        # BUSCA SIMPLES - Usa o Spotify search normal
        query = f'track:"{track_name}" artist:"{artist_name}"'
        print(f"Ã°Å¸â€Â Searching: {query}")
        
        results = sp.search(q=query, type='track', limit=50)
        
        if results['tracks']['items']:
            # PEGAR O PRIMEIRO ELEMENTO DO ARRAY
            track = results['tracks']['items'][0]
            
            print(f"   Found: {track['name']} - {track['artists'][0]['name']}")
            
            # Get image URL com acesso correcto ao array
            image_url = None
            if track['album']['images']:
                images = track['album']['images']
                # images ÃƒÂ© ARRAY, usa ÃƒÂ­ndice 1 ou 0
                image_url = images[1]['url'] if len(images) > 1 else images[0]['url']
            
            return {
                'id': track['id'],
                'name': track['name'],
                'artist': track['artists'][0]['name'],
                'uri': track['uri'],
                'spotify_url': track['external_urls']['spotify'],
                'image_url': image_url,
                'preview_url': track['preview_url']
            }
        
        print(f"   Ã¢ÂÅ’ No results found")
        return None
    
    except Exception as e:
        print(f"Ã¢ÂÅ’ Search error for '{track_name}': {e}")
        import traceback
        traceback.print_exc()
        return None



def enhance_data_with_spotify_ids(data, data_type='track'):
    """Search Spotify IDs for all items - FIXED VERSION"""
    sp = get_spotify_client()
    if not sp:
        return data
    
    enhanced_data = []
    print(f"🎵 Enriquecendo {min(len(data), 100)} {data_type}s com imagens...")
    
    for i, item in enumerate(data[:100]):  # Limite 100
        enhanced_item = item.copy()
        
        try:
            if data_type == 'track':
                # ✅ CORRIGIR: Parse correto do track_key
                track_key = item.get('track_key', '')
                if ' - ' in track_key:
                    track_name, artist_name = track_key.split(' - ', 1)
                else:
                    track_name = track_key
                    artist_name = 'Unknown'
                
                print(f"  [{i+1}/{min(len(data), 100)}] Searching: {track_name} - {artist_name}")
                
                track_data = search_track_get_id(track_name.strip(), artist_name.strip())
                
                if track_data:
                    enhanced_item['id'] = track_data.get('id')
                    enhanced_item['uri'] = track_data.get('uri')
                    enhanced_item['spotify_url'] = track_data.get('spotify_url')
                    enhanced_item['image_url'] = track_data.get('image_url')
                    print(f"    ✅ Imagem encontrada")
                else:
                    print(f"    ❌ Não encontrado")
            
            elif data_type == 'artist':
                artist_name = item.get('artist_key', '')
                results = sp.search(q=f'artist:"{artist_name}"', type='artist', limit=1)
                
                if results['artists']['items']:
                    artist = results['artists']['items'][0]
                    enhanced_item['artist_id'] = artist['id']
                    enhanced_item['spotify_url'] = artist['external_urls']['spotify']
                    if artist['images']:
                        enhanced_item['image_url'] = artist['images'][0]['url']
            
            elif data_type == 'album':
                album_name = item.get('album_key', '')
                results = sp.search(q=f'album:"{album_name}"', type='album', limit=1)
                
                if results['albums']['items']:
                    album = results['albums']['items'][0]
                    enhanced_item['album_id'] = album['id']
                    enhanced_item['spotify_url'] = album['external_urls']['spotify']
                    if album['images']:
                        enhanced_item['image_url'] = album['images'][0]['url']
        
        except Exception as e:
            print(f"    ❌ Erro: {e}")
        
        enhanced_data.append(enhanced_item)
    
    # Se houver mais de 100, adicionar os restantes sem enriquecimento
    if len(data) > 100:
        enhanced_data.extend(data[100:])
    
    ids_found = sum(1 for item in enhanced_data if item.get('id') or item.get('image_url'))
    print(f"✅ Enriquecidos {ids_found}/{len(data)} items com metadados\n")
    
    return enhanced_data


def apply_filters(df, year_filter=None, month_filter=None):
    """Apply year and month filters"""
    if df.empty:
        return df
    
    filtered_df = df.copy()
    
    if year_filter and year_filter != 'all':
        filtered_df = filtered_df[filtered_df['ts'].dt.year == int(year_filter)]
    
    if month_filter and month_filter != 'all':
        filtered_df = filtered_df[filtered_df['ts'].dt.month == int(month_filter)]
    
    return filtered_df

def search_tracks_for_playlist(track_keys):
    """Search tracks on Spotify for playlist creation"""
    sp = get_spotify_client()
    if not sp:
        print("Ã¢ÂÅ’ Spotify client not available")
        return []
    
    found_tracks = []
    
    print(f"Ã°Å¸â€Â Searching {len(track_keys)} tracks on Spotify...")
    
    for i, track_key in enumerate(track_keys):  # Use ALL tracks provided
        try:
            # Parse track_key (format: "Track Name - Artist Name")
            if ' - ' in track_key:
                track_name, artist_name = track_key.split(' - ', 1)
            else:
                track_name = track_key
                artist_name = ''
            
            # Use optimized search function
            track_data = search_track_get_id(track_name, artist_name)
            
            if track_data:
                found_tracks.append(track_data['uri'])
                print(f"  Ã¢Å“â€¦ [{i+1}/{len(track_keys)}] Found: {track_data['name']} - {track_data['artist']}")
            else:
                print(f"  Ã¢ÂÅ’ [{i+1}/{len(track_keys)}] Not found: {track_key}")
                    
        except Exception as e:
            print(f"    Ã¢ÂÅ’ Error searching {track_key}: {e}")
            continue
    
    print(f"Ã¢Å“â€¦ {len(found_tracks)} tracks found out of {len(track_keys)} requested")
    return found_tracks

# ========== CORRECT ANALYTICS FUNCTIONS ==========

def repeat_spirals_max_single_day(df, n=50, time_period='all'):
    """
    REPEAT SPIRALS: O nÃƒÂºmero mÃƒÂ¡ximo de vezes que ouviste uma mÃƒÂºsica NUM SÃƒâ€œ DIA/SEMANA/MÃƒÅ S no perÃƒÂ­odo
    APENAS com plays INTENTIONAL (tu escolheste a mÃƒÂºsica)

    Args:
        df: DataFrame filtrado
        n: NÃƒÂºmero de resultados
        time_period: 'day', 'week', 'month', ou 'all'
    """
    if df.empty:
        return []

    # FILTRAR APENAS PLAYS INTENTIONAL
    df_intentional = df[df['play_type'] == 'INTENTIONAL'].copy()

    if df_intentional.empty:
        return []

    # Add date column
    df_intentional['date'] = df_intentional['ts'].dt.date

    # Group by track_key and date/week/month based on period
    if time_period == 'day':
        # Max plays in a single day
        daily_plays = df_intentional.groupby(['track_key', 'date']).size().reset_index(name='plays_per_period')
    elif time_period == 'week':
        # Max plays in a single week
        df_intentional['week'] = df_intentional['ts'].dt.to_period('W').apply(lambda r: r.start_time)
        daily_plays = df_intentional.groupby(['track_key', 'week']).size().reset_index(name='plays_per_period')
    elif time_period == 'month':
        # Max plays in a single month
        df_intentional['month'] = df_intentional['ts'].dt.to_period('M').apply(lambda r: r.start_time)
        daily_plays = df_intentional.groupby(['track_key', 'month']).size().reset_index(name='plays_per_period')
    else:  # 'all'
        daily_plays = df_intentional.groupby(['track_key', 'date']).size().reset_index(name='plays_per_period')

    # For each track, find the MAXIMUM plays in the period
    max_single_period = daily_plays.groupby('track_key')['plays_per_period'].max().reset_index()
    max_single_period.columns = ['track_key', 'max_plays_single_period']

    # Sort by max plays and return top n
    result = max_single_period.sort_values('max_plays_single_period', ascending=False).head(n)

    return [(row['track_key'], row['max_plays_single_period']) for _, row in result.iterrows()]

def consecutive_days_listening(df, n=100):
    """
    REPEAT DAYS: O nÃƒÂºmero mÃƒÂ¡ximo de DIAS SEGUIDOS que ouviste uma mÃƒÂºsica no perÃƒÂ­odo
    """
    if df.empty:
        return []
    
    # Add date column
    df_copy = df.copy()
    df_copy['date'] = df_copy['ts'].dt.date
    
    # Get unique dates per track
    track_dates = df_copy.groupby('track_key')['date'].apply(lambda x: sorted(x.unique())).reset_index()
    
    results = []
    
    for _, row in track_dates.iterrows():
        track_key = row['track_key']
        dates = row['date']
        
        if len(dates) == 0:
            continue
        
        # Find longest consecutive sequence
        max_consecutive = 1
        current_consecutive = 1
        
        for i in range(1, len(dates)):
            # Check if dates are consecutive (1 day apart)
            if (dates[i] - dates[i-1]).days == 1:
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                current_consecutive = 1
        
        results.append((track_key, max_consecutive))
    
    # Sort by consecutive days and return top n
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:n]


def top_tracks_really_played(df, n=100):
    """
    TOP TRACKS com APENAS plays INTENTIONAL (REALLY PLAYED)
    Filtra apenas mÃƒÂºsicas que tu escolheste ouvir
    """
    if df.empty:
        return []

    # FILTRAR APENAS PLAYS INTENTIONAL
    df_intentional = df[df['play_type'] == 'INTENTIONAL'].copy()

    if df_intentional.empty:
        return []

    # Contar plays por track
    track_counts = (
        df_intentional.groupby('track_key', sort=False)
        .size()
        .reset_index(name='plays')
        .sort_values('plays', ascending=False)
        .head(n)
    )

    return [(row['track_key'], row['plays']) for _, row in track_counts.iterrows()]



def top_artists_really_played(df, n=100):
    """
    TOP ARTISTS com APENAS plays INTENTIONAL (REALLY PLAYED)
    Filtra apenas mÃƒÂºsicas que tu escolheste ouvir
    """
    if df.empty:
        return []

    # FILTRAR APENAS PLAYS INTENTIONAL
    df_intentional = df[df['play_type'] == 'INTENTIONAL'].copy()

    if df_intentional.empty:
        return []

    # Contar plays por artista
    artist_counts = (
        df_intentional.groupby('artist_key', sort=False)
        .size()
        .reset_index(name='plays')
        .sort_values('plays', ascending=False)
        .head(n)
    )

    return [(row['artist_key'], row['plays']) for _, row in artist_counts.iterrows()]


def top_albums_really_played(df, n=100):
    """
    TOP ALBUMS com APENAS plays INTENTIONAL (REALLY PLAYED)
    Filtra apenas mÃƒÂºsicas que tu escolheste ouvir
    """
    if df.empty:
        return []

    # FILTRAR APENAS PLAYS INTENTIONAL
    df_intentional = df[df['play_type'] == 'INTENTIONAL'].copy()

    if df_intentional.empty:
        return []

    # Contar plays por album
    album_counts = (
        df_intentional.groupby('album_key', sort=False)
        .size()
        .reset_index(name='plays')
        .sort_values('plays', ascending=False)
        .head(n)
    )

    return [(row['album_key'], row['plays']) for _, row in album_counts.iterrows()]

def get_track_calendar_data(df, track_key):
    """Get all dates when a specific track was played (for calendar view) - FULL DATA"""
    if df.empty:
        return {}
    
    # Filter for specific track
    track_data = df[df['track_key'] == track_key].copy()
    
    if track_data.empty:
        return {}
    
    # Add date column
    track_data['date'] = track_data['ts'].dt.date
    
    # Group by date and count plays
    daily_plays = track_data.groupby('date').size().reset_index(name='plays')
    
    # Convert to dictionary with date as key and plays as value
    calendar_data = {}
    for _, row in daily_plays.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        calendar_data[date_str] = int(row['plays'])
    
    return calendar_data

# ========== MAIN ROUTES ==========

# ============================================================================
# ROTAS MULTI-USER (UPLOAD + AUTH)
# ============================================================================

@app.route('/api/save-username', methods=['POST'])
def save_username():
    """Save username E criar user_id"""
    data = request.get_json()
    username = data.get('username', '').strip()
    
    if not username or len(username) < 2:
        return jsonify({'success': False, 'error': 'Invalid username'}), 400
    
    # Ã¢Å“â€¦ CRIAR user_id AQUI (sÃƒÂ³ quando user guarda username)
    if 'user_id' not in session:
        session['user_id'] = str(uuid.uuid4())
        print(f"\nÃ°Å¸â€ â€ NEW user_id created: {session['user_id']}")
    
    session['username'] = username
    session.modified = True
    
    print(f"Ã¢Å“â€¦ Username saved: {username}")
    print(f"   user_id: {session['user_id'][:8]}...")
    
    return jsonify({'success': True, 'username': username}), 200


@app.route('/upload', methods=['POST'])
def upload_files():
    """Upload de ficheiros JSON individuais"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        user_folder = get_user_folder()
        filepath = os.path.join(user_folder, filename)
        
        file.save(filepath)
        print(f"Ã¢Å“â€¦ File uploaded: {filename}")
        
        return jsonify({
            'success': True,
            'filename': filename,
            'message': 'File uploaded successfully'
        }), 200
    
    return jsonify({'error': 'Invalid file type. Only .json allowed'}), 400

@app.route('/upload-complete', methods=['POST'])
def upload_complete():
    """Marca upload completo e redireciona para auth"""
    user_folder = get_user_folder()
    json_files = [f for f in os.listdir(user_folder) if f.endswith('.json')]
    
    if not json_files:
        return jsonify({'error': 'No JSON files uploaded'}), 400
    
    session['files_uploaded'] = True
    session['file_count'] = len(json_files)
    session.modified = True
    
    print(f"Ã¢Å“â€¦ Upload complete: {len(json_files)} files")
    
    return jsonify({
        'success': True,
        'message': f'{len(json_files)} files uploaded',
        'redirect_url': url_for('spotify_auth')
    }), 200

@app.route('/spotify-auth')
def spotify_auth():
    """Inicia OAuth flow"""
    if not session.get('files_uploaded'):
        return redirect(url_for('home'))
    
    cache_path = os.path.join(get_user_folder(), '.spotify_cache')
    
    redirect_uri = Config.REDIRECT_URI  # Ã¢Å“â€¦ ADICIONA
    
    auth_manager = SpotifyOAuth(
        client_id=Config.SPOTIFY_CLIENT_ID,
        client_secret=Config.SPOTIFY_CLIENT_SECRET,
        redirect_uri=redirect_uri,  # Ã¢Å“â€¦ USA DINÃƒâ€šMICO
        scope='user-top-read playlist-modify-public playlist-modify-private streaming user-read-private user-modify-playback-state user-read-playback-state',
        cache_path=cache_path,
        show_dialog=True
    )
    
    auth_url = auth_manager.get_authorize_url()
    return redirect(auth_url)

@app.route('/callback')
def callback():
    """OAuth callback - processa autenticaÃƒÂ§ÃƒÂ£o Spotify"""
    code = request.args.get('code')
    
    if not code:
        print("Ã¢ÂÅ’ No code in callback")
        return redirect(url_for('home'))
    
    # DEBUG
    print("\n" + "="*70)
    print("CALLBACK START")
    print("="*70)
    print(f"user_id: {session.get('user_id', 'NONE')[:8] if session.get('user_id') else 'NONE'}...")
    print(f"username: {session.get('username', 'NONE')}")
    print(f"files_uploaded: {session.get('files_uploaded', False)}")
    print("="*70)
    
    # Cache path
    if 'user_id' in session and session.get('files_uploaded'):
        try:
            user_folder = get_user_folder()
            cache_path = os.path.join(user_folder, '.spotify_cache')
            
            # Verificar ficheiros
            json_files = [f for f in os.listdir(user_folder) if f.endswith('.json')]
            print(f"Ã°Å¸â€œÂ User folder: {user_folder}")
            print(f"Ã°Å¸â€œâ€ž JSON files: {len(json_files)}")
            
        except Exception as e:
            print(f"Ã¢ÂÅ’ Error: {e}")
            cache_path = '.cache-default'
    else:
        cache_path = '.cache-default'
        print("Ã°Å¸â€œÂ Using default cache")
    
    # OAuth
    redirect_uri = Config.REDIRECT_URI # Ã¢Å“â€¦ ADICIONA
    
    auth_manager = SpotifyOAuth(
        client_id=Config.SPOTIFY_CLIENT_ID,
        client_secret=Config.SPOTIFY_CLIENT_SECRET,
        redirect_uri=redirect_uri,  # Ã¢Å“â€¦ USA DINÃƒâ€šMICO
        scope='user-top-read playlist-modify-public playlist-modify-private streaming user-read-private user-modify-playback-state user-read-playback-state',
        cache_path=cache_path
    )
    
    try:
        token_info = auth_manager.get_access_token(code, as_dict=True, check_cache=False)
        
        if not token_info:
            raise ValueError("Failed to get token")
        
        print("Ã¢Å“â€¦ Spotify token obtained")
        session['spotify_authenticated'] = True
        
        # CARREGAR DADOS
        if session.get('files_uploaded'):
            print("\nÃ°Å¸â€œÅ  Loading uploaded files...")
            
            try:
                df_music = load_local_data()
                
                if not df_music.empty:
                    session['data_loaded'] = True
                    print(f"Ã¢Å“â€¦ {len(df_music):,} records loaded")
                else:
                    print("Ã¢ÂÅ’ DataFrame empty!")
                    
            except Exception as e:
                print(f"Ã¢ÂÅ’ Error: {e}")
                import traceback
                traceback.print_exc()
        else:
            # Dev mode
            print("\nÃ°Å¸â€œÅ  Dev mode...")
            df_music = load_local_data()
            if not df_music.empty:
                session['data_loaded'] = True
                print(f"Ã¢Å“â€¦ {len(df_music):,} records")
        
        session.modified = True
        
        print("\n" + "="*70)
        print("CALLBACK END")
        print(f"data_loaded: {session.get('data_loaded')}")
        print(f"spotify_authenticated: {session.get('spotify_authenticated')}")
        print("="*70 + "\n")
        
        response = app.make_response(redirect(url_for('home')))
        return response
        
    except Exception as e:
        print(f"\nÃ¢ÂÅ’ CALLBACK ERROR: {e}")
        import traceback
        traceback.print_exc()
        return redirect(url_for('home'))


@app.route('/logout')
def logout():
    """Limpa sessÃƒÂ£o e dados do utilizador"""
    if 'user_id' in session:
        user_folder = os.path.join(Config.UPLOAD_FOLDER, session['user_id'])
        if os.path.exists(user_folder):
            shutil.rmtree(user_folder)
    
    session.clear()
    return redirect(url_for('home'))


@app.route('/api/artist_top_tracks')
def get_artist_top_tracks():
    """Get top 10 tracks for a specific artist"""
    artist_name = request.args.get('artist_name', '').strip()
    df = load_local_data()
    
    if df.empty:
        return jsonify({'success': False, 'error': 'No data available'})

    try:
        df_artist = df[df['master_metadata_album_artist_name'].str.strip() == artist_name].copy()
        
        if df_artist.empty:
            return jsonify({'success': False, 'error': f'No tracks found for artist: {artist_name}'})

        # Get top 10 tracks
        top_tracks_list = (
            df_artist.groupby('track_key', sort=False)
            .size()
            .reset_index(name='plays')
            .sort_values('plays', ascending=False)
            .head(10)
        )

        # Format result
        result = []
        for idx, (_, row) in enumerate(top_tracks_list.iterrows(), 1):
            track_key = row['track_key']
            track_name, artist = track_key.split(' - ', 1) if ' - ' in track_key else (track_key, artist_name)
            result.append({
                'rank': idx,
                'track_key': track_key,
                'name': track_name.strip(),
                'artist': artist.strip(),
                'plays': int(row['plays']),
                'image_url': None,
                'spotify_url': '',    # ← ADICIONA
                'preview_url': '',    # ← ADICIONA
                'uri': '',            # ← ADICIONA
                'id': ''              # ← ADICIONA
            })

        # ✅ ADICIONAR: Enriquecer TODAS as 10 com imagens (limite máximo 100)
        print(f"🎵 Enriquecendo TODAS as 10 músicas com imagens para artist: {artist_name}")
        for i, item in enumerate(result[:100]):
            try:
                track_data = search_track_get_id(item['name'], item['artist'])
                if track_data:
                    # ADICIONAR TODOS OS CAMPOS NECESSÁRIOS
                    item['id'] = track_data.get('id')
                    item['uri'] = track_data.get('uri')
                    item['spotify_url'] = track_data.get('spotify_url')
                    item['preview_url'] = track_data.get('preview_url')
                    item['image_url'] = track_data.get('image_url')
                    print(f"  ✅ [{i+1}] {item['name']} - dados completos")
            except Exception as e:
                print(f"  ❌ Erro na track {i+1}: {e}")
                pass

        return jsonify({
            'success': True,
            'data': result,
            'artist_name': artist_name
        })
    
    except Exception as e:
        print(f"Error getting artist top tracks: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/album_top_tracks')
def get_album_top_tracks():
    """Get top 10 tracks for a specific album"""
    album_name = request.args.get('album_name', '').strip()
    df = load_local_data()
    
    if df.empty:
        return jsonify({'success': False, 'error': 'No data available'})

    try:
        df_album = df[df['master_metadata_album_album_name'].str.strip() == album_name].copy()
        
        if df_album.empty:
            all_albums = df['master_metadata_album_album_name'].str.strip().unique()
            similar = [a for a in all_albums if album_name.lower() in a.lower()][:5]
            print(f"❌ Album '{album_name}' não encontrado. Similares: {similar}")
            return jsonify({'success': False, 'error': f'No tracks found for album: {album_name}'})

        # Get top 10 tracks
        top_tracks_list = (
            df_album.groupby('track_key', sort=False)
            .size()
            .reset_index(name='plays')
            .sort_values('plays', ascending=False)
            .head(10)
        )

        # Format result
        result = []
        for idx, (_, row) in enumerate(top_tracks_list.iterrows(), 1):
            track_key = row['track_key']
            track_name, artist = track_key.split(' - ', 1) if ' - ' in track_key else (track_key, 'Unknown')
            result.append({
                'rank': idx,
                'track_key': track_key,
                'name': track_name.strip(),
                'artist': artist.strip(),
                'plays': int(row['plays']),
                'image_url': None,
                'spotify_url': '',    # ← ADICIONA
                'preview_url': '',    # ← ADICIONA
                'uri': '',            # ← ADICIONA
                'id': ''              # ← ADICIONA
            })

        # ✅ ADICIONAR: Enriquecer TODAS as 10 com imagens (limite máximo 100)
        print(f"🎵 Enriquecendo TODAS as 10 músicas com imagens para album: {album_name}")
        for i, item in enumerate(result[:100]):
            try:
                track_data = search_track_get_id(item['name'], item['artist'])
                if track_data:
                    # ADICIONAR TODOS OS CAMPOS NECESSÁRIOS
                    item['id'] = track_data.get('id')
                    item['uri'] = track_data.get('uri')
                    item['spotify_url'] = track_data.get('spotify_url')
                    item['preview_url'] = track_data.get('preview_url')
                    item['image_url'] = track_data.get('image_url')
                    print(f"  ✅ [{i+1}] {item['name']} - dados completos")
            except Exception as e:
                print(f"  ❌ Erro na track {i+1}: {e}")
                pass

        return jsonify({
            'success': True,
            'data': result,
            'album_name': album_name
        })
    
    except Exception as e:
        print(f"Error getting album top tracks: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)})

@app.route('/dashboard')
def dashboard():
    """Dashboard route with data validation"""
    # ✅ VALIDAR se tem dados
    df = load_local_data()
    
    if df.empty:
        # ❌ Sem dados, redirecionar para landing/home
        print("⚠️ Dashboard: No data available, redirecting to home")
        session['data_loaded'] = False
        return redirect(url_for('home'))
    
    # ✅ Tem dados, renderizar dashboard
    print(f"✅ Dashboard: Rendering with {len(df):,} records")
    return render_template('dashboard.html', username=session.get('username', 'User'))


@app.route('/')
def home():
    """Landing page ou dashboard"""
    # ✅ DEBUG
    print(f"\n[HOME] files_uploaded: {session.get('files_uploaded')}")
    print(f"[HOME] data_loaded: {session.get('data_loaded')}")
    print(f"[HOME] spotify_authenticated: {session.get('spotify_authenticated')}\n")
    
    # CÓDIGO NOVO
    if session.get('files_uploaded'):
        # ✅ VERIFICAR SE DADOS EXISTEM EM DISCO (não confiar só na session)
        try:
            df = load_local_data()
            has_data = not df.empty
        except:
            has_data = False
        
        # ✅ Verificar autenticação Spotify
        sp = get_spotify_client()
        is_authenticated = sp is not None
        
        print(f"[HOME] Data check from disk: {has_data}")
        print(f"[HOME] Spotify auth check: {is_authenticated}")
        
        # ✅ Se tem DADOS (em disco) E autenticação → dashboard
        if has_data and is_authenticated:
            # Garantir que session está atualizada
            session['data_loaded'] = True
            session['spotify_authenticated'] = True
            session.modified = True
            
            print("✅ Redirecting to /dashboard (multi-user mode)")
            return redirect(url_for('dashboard'))
        else:
            print(f"⚠️ Rendering landing: has_data={has_data}, is_auth={is_authenticated}")
            return render_template('landing.html')

    else:
        # Dev mode
        df = load_local_data()
        if not df.empty:
            sp = get_spotify_client()
            if sp:
                print("✅ Redirecting to /dashboard (dev mode)")
                return redirect(url_for('dashboard'))  # ✅ REDIRECIONAR para /dashboard
            else:
                # HTML inline para dev redirect
                redirect_uri = Config.REDIRECT_URI
                auth_manager = SpotifyOAuth(
                    client_id=Config.SPOTIFY_CLIENT_ID,
                    client_secret=Config.SPOTIFY_CLIENT_SECRET,
                    redirect_uri=redirect_uri,
                    scope='streaming user-read-private user-read-email user-top-read user-read-recently-played user-library-read playlist-modify-public playlist-modify-private user-modify-playback-state user-read-playback-state',
                    cache_path='.cache-dev'
                )
                auth_url = auth_manager.get_authorize_url()
                return f'''
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Connect Spotify</title>
                    <style>
                        body {{
                            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                            display: flex;
                            justify-content: center;
                            align-items: center;
                            height: 100vh;
                            margin: 0;
                        }}
                        .container {{
                            background: white;
                            padding: 3rem;
                            border-radius: 20px;
                            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                            text-align: center;
                            max-width: 500px;
                        }}
                        h1 {{ color: #333; margin-bottom: 1rem; }}
                        p {{ color: #666; margin-bottom: 2rem; }}
                        .btn {{
                            background: #1DB954;
                            color: white;
                            padding: 15px 40px;
                            border-radius: 50px;
                            text-decoration: none;
                            font-weight: bold;
                            font-size: 1.1rem;
                            display: inline-block;
                            transition: transform 0.2s;
                        }}
                        .btn:hover {{ transform: scale(1.05); }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>Ã°Å¸Å½Âµ Spotify Dashboard</h1>
                        <p>Connect your Spotify account</p>
                        <a href="{auth_url}" class="btn">Connect Spotify</a>
                    </div>
                </body>
                </html>
                '''
        
        print("Ã¢â€ â€™ Rendering landing (no data)")
        return render_template('landing.html')



# ========== API SEARCH ENDPOINT ==========

@app.route('/api/search_track')
def api_search_track():
    """Search for specific track on Spotify and return ID"""
    track_name = request.args.get('track_name', '')
    artist_name = request.args.get('artist_name', '')
    
    if not track_name:
        return jsonify({'success': False, 'error': 'Track name required'})
    
    print(f"Ã°Å¸â€Â API Search request: {track_name} - {artist_name}")
    
    track_data = search_track_get_id(track_name, artist_name)
    
    if track_data:
        print(f"Ã¢Å“â€¦ API Search success: {track_data['name']} - {track_data['artist']} (ID: {track_data['id']})")
        return jsonify({
            'success': True,
            'track': track_data
        })
    else:
        print(f"Ã¢ÂÅ’ API Search failed: {track_name} - {artist_name}")
        return jsonify({
            'success': False,
            'error': 'Track not found on Spotify'
        })

# ========== CORRECTED API ENDPOINTS ==========

@app.route('/api/track_calendar')
def api_track_calendar():
    """Get calendar data for a specific track - FULL DATA (no filters)"""
    track_key = request.args.get('track_key', '')
    
    if not track_key:
        return jsonify({'success': False, 'error': 'Track key required'})
    
    try:
        df_music = load_local_data()  # Full data, no filters
        calendar_data = get_track_calendar_data(df_music, track_key)
        
        # Get track info
        track_info = {}
        if ' - ' in track_key:
            track_name, artist_name = track_key.split(' - ', 1)
            track_info = {
                'name': track_name,
                'artist': artist_name,
                'track_key': track_key
            }
        
        return jsonify({
            'success': True,
            'calendar_data': calendar_data,
            'track_info': track_info
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/local_tracks')
def api_local_tracks():
    """Top tracks from local data with filters and IDs - TOP 50"""
    try:
        df_music = load_local_data()
        year_filter = request.args.get('year', 'all')
        month_filter = request.args.get('month', 'all')
        
        # Apply filters
        filtered_df = apply_filters(df_music, year_filter, month_filter)
        
        # Get top tracks - TOP 50
        limit = int(request.args.get('limit', 10))  # Default 10
        tracks_data = top_tracks(filtered_df, n=limit, include_metadata=True)
        
        # Convert to list of dictionaries
        tracks_list = []
        for _, track in tracks_data.iterrows():
            tracks_list.append({
                'track_key': track['track_key'],
                'plays': int(track['plays']),
                'spotify_url': track.get('spotify_url', ''),
                'image_url': track.get('image_url', ''),
                'enhanced_name': track.get('enhanced_name', ''),
                'enhanced_artist': track.get('enhanced_artist', ''),
                'preview_url': track.get('preview_url', ''),
                'uri': track.get('uri', ''),
                'track_id': track.get('track_id', ''),
                'id': track.get('id', '')
            })
        
        # Search Spotify IDs for ALL tracks
        tracks_with_ids = enhance_data_with_spotify_ids(tracks_list, 'track')
        
        return jsonify({'success': True, 'data': tracks_with_ids})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/local_artists')
def api_local_artists():
    """Top artists from local data with filters and IDs - TOP 50"""
    try:
        df_music = load_local_data()
        year_filter = request.args.get('year', 'all')
        month_filter = request.args.get('month', 'all')
        
        filtered_df = apply_filters(df_music, year_filter, month_filter)
        limit = int(request.args.get('limit', 10))  # Default 10
        artists_data = top_artists(filtered_df, n=limit, include_metadata=True)

        artists_list = []
        for _, artist in artists_data.iterrows():
            artists_list.append({
                'artist_key': artist['artist_key'],
                'plays': int(artist['plays']),
                'spotify_url': artist.get('spotify_url', ''),
                'image_url': artist.get('image_url', ''),
                'enhanced_name': artist.get('enhanced_name', '')
            })
        
        # Search Spotify IDs for ALL artists
        artists_with_ids = enhance_data_with_spotify_ids(artists_list, 'artist')
        
        return jsonify({'success': True, 'data': artists_with_ids})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/local_albums')
def api_local_albums():
    """Top albums from local data with filters and IDs - TOP 50"""
    try:
        df_music = load_local_data()
        year_filter = request.args.get('year', 'all')
        month_filter = request.args.get('month', 'all')
        
        filtered_df = apply_filters(df_music, year_filter, month_filter)
        limit = int(request.args.get('limit', 10))  # Default 10
        albums_data = top_albums(filtered_df, n=limit, include_metadata=True)

        albums_list = []
        for _, album in albums_data.iterrows():
            albums_list.append({
                'album_key': album['album_key'],
                'plays': int(album['plays']),
                'spotify_url': album.get('spotify_url', ''),
                'image_url': album.get('image_url', ''),
                'enhanced_name': album.get('enhanced_name', '')
            })
        
        # Search Spotify IDs for ALL albums
        albums_with_ids = enhance_data_with_spotify_ids(albums_list, 'album')
        
        return jsonify({'success': True, 'data': albums_with_ids})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/local_tracks_really_played')
def api_local_tracks_really_played():
    """Top tracks REALLY PLAYED - APENAS PLAYS INTENTIONAL with filters and IDs - TOP 50"""
    try:
        df_music = load_local_data()
        year_filter = request.args.get('year', 'all')
        month_filter = request.args.get('month', 'all')

        # Apply filters
        filtered_df = apply_filters(df_music, year_filter, month_filter)

        # Get really played tracks
        limit = int(request.args.get('limit', 10))  # Default 10
        tracks_data = top_tracks_really_played(filtered_df, n=limit)


        # Convert to list of dictionaries
        tracks_list = []
        for track_key, plays in tracks_data:
            tracks_list.append({
                'track_key': track_key,
                'plays': plays,
                'spotify_url': '',           # ← ADICIONA
                'image_url': '',             # ← ADICIONA
                'preview_url': '',           # ← ADICIONA
                'uri': '',                   # ← ADICIONA
                'id': ''                     # ← ADICIONA
            })

        # Search Spotify IDs for ALL tracks
        tracks_with_ids = enhance_data_with_spotify_ids(tracks_list, 'track')

        return jsonify({'success': True, 'data': tracks_with_ids})  # Ã¢Å“â€¦ CORRETO
    except Exception as e:
        return jsonify(success=False, error=str(e))


@app.route('/api/local_artists_really_played')
def api_local_artists_really_played():
    """Top artists REALLY PLAYED - APENAS PLAYS INTENTIONAL with filters and IDs - TOP 50"""
    try:
        df_music = load_local_data()
        year_filter = request.args.get('year', 'all')
        month_filter = request.args.get('month', 'all')

        # Apply filters
        filtered_df = apply_filters(df_music, year_filter, month_filter)

        # Get really played artists
        limit = int(request.args.get('limit', 10))  # Default 10
        artists_data = top_artists_really_played(filtered_df, n=limit)


        # Convert to list of dictionaries
        artists_list = []
        for artist_key, plays in artists_data:
            artists_list.append({
                'artist_key': artist_key,
                'enhanced_name': artist_key,
                'plays': int(plays)
            })

        # Search Spotify IDs for artists
        artists_with_ids = enhance_data_with_spotify_ids(artists_list, 'artist')

        return jsonify({'success': True, 'data': artists_with_ids})

    except Exception as e:
        return jsonify(success=False, error=str(e))


@app.route('/api/local_albums_really_played')
def api_local_albums_really_played():
    """Top albums REALLY PLAYED - APENAS PLAYS INTENTIONAL with filters and IDs - TOP 50"""
    try:
        df_music = load_local_data()
        year_filter = request.args.get('year', 'all')
        month_filter = request.args.get('month', 'all')

        # Apply filters
        filtered_df = apply_filters(df_music, year_filter, month_filter)

        # Get really played albums
        limit = int(request.args.get('limit', 10))  # Default 10
        albums_data = top_albums_really_played(filtered_df, n=limit)


        # Convert to list of dictionaries
        albums_list = []
        for album_key, plays in albums_data:
            albums_list.append({
                'album_key': album_key,
                'enhanced_name': album_key,
                'plays': int(plays)
            })

        # Search Spotify IDs for albums
        albums_with_ids = enhance_data_with_spotify_ids(albums_list, 'album')

        return jsonify({'success': True, 'data': albums_with_ids})

    except Exception as e:
        return jsonify(success=False, error=str(e))


@app.route('/api/daily_history')
def api_daily_history():
    """Daily history with filters"""
    try:
        df_music = load_local_data()
        year_filter = request.args.get('year', 'all')
        month_filter = request.args.get('month', 'all')
        
        filtered_df = apply_filters(df_music, year_filter, month_filter)
        history_data = daily_history(filtered_df)
        
        # Convert to JSON format
        history_list = []
        for _, day in history_data.iterrows():
            history_list.append({
                'date': day['date'].strftime('%Y-%m-%d'),
                'plays': int(day['plays'])
            })
        
        return jsonify({'success': True, 'data': history_list})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/repeat_spirals')
def api_repeat_spirals():
    """REPEAT SPIRALS: Max plays in a single day/week/month with filters and IDs - TOP 50
    APENAS PLAYS INTENTIONAL"""
    try:
        df_music = load_local_data()
        year_filter = request.args.get('year', 'all')
        month_filter = request.args.get('month', 'all')
        time_period = request.args.get('period', 'all')  # 'day', 'week', 'month', 'all'

        # Apply filters
        filtered_df = apply_filters(df_music, year_filter, month_filter)

        # Get spirals data with time period filter
        limit = int(request.args.get('limit', 10))  # Default 10
        spirals_data = repeat_spirals_max_single_day(filtered_df, n=limit, time_period=time_period)

        # Convert to JSON format
        spirals_list = []
        for track_key, max_single_day in spirals_data:
            spirals_list.append({
                'track_key': track_key,
                'max_single_day_plays': max_single_day,
                'spotify_url': '',           # ← ADICIONA
                'image_url': '',             # ← ADICIONA
                'preview_url': '',           # ← ADICIONA
                'uri': '',                   # ← ADICIONA
                'id': ''                     # ← ADICIONA
            })

        # Search Spotify IDs for ALL tracks
        spirals_with_ids = enhance_data_with_spotify_ids(spirals_list, 'track')
        
        # ✅ ADICIONAR: Enriquecer com imagens até 100
        print(f"🎵 Enriquecendo {min(len(spirals_with_ids), 100)} repeat spirals com imagens...")
        for i, item in enumerate(spirals_with_ids[:100]):  # Limite 100
            if not item.get('image_url'):  # Só se ainda não tem imagem
                try:
                    track_name, artist = item['track_key'].split(' - ', 1) if ' - ' in item['track_key'] else (item['track_key'], 'Unknown')
                    track_data = search_track_get_id(track_name, artist)
                    if track_data:
                        item['image_url'] = track_data.get('image_url')
                        print(f"  ✅ [{i+1}] {track_name} - imagem encontrada")
                except Exception as e:
                    print(f"  ❌ Erro no item {i+1}: {e}")
                    pass

        return jsonify({'success': True, 'data': spirals_with_ids})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/repeat_days')
def api_repeat_days():
    """REPEAT DAYS: Max consecutive days with filters and IDs - TOP 50"""
    try:
        df_music = load_local_data()
        year_filter = request.args.get('year', 'all')
        month_filter = request.args.get('month', 'all')
        
        filtered_df = apply_filters(df_music, year_filter, month_filter)
        limit = int(request.args.get('limit', 10))  # Default 10
        days_data = consecutive_days_listening(filtered_df, n=limit)
        
        days_list = []
        for track_key, consecutive_days in days_data:
            days_list.append({
                'track_key': track_key,
                'consecutive_days': consecutive_days,
                'spotify_url': '',           # ← ADICIONA
                'image_url': '',             # ← ADICIONA
                'preview_url': '',           # ← ADICIONA
                'uri': '',                   # ← ADICIONA
                'id': ''                     # ← ADICIONA
            })
        
        # Search Spotify IDs for ALL tracks
        days_with_ids = enhance_data_with_spotify_ids(days_list, 'track')
        
        # ✅ ADICIONAR: Enriquecer com imagens até 100
        print(f"🎵 Enriquecendo {min(len(days_with_ids), 100)} repeat days com imagens...")
        for i, item in enumerate(days_with_ids[:100]):  # Limite 100
            if not item.get('image_url'):  # Só se ainda não tem imagem
                try:
                    track_name, artist = item['track_key'].split(' - ', 1) if ' - ' in item['track_key'] else (item['track_key'], 'Unknown')
                    track_data = search_track_get_id(track_name, artist)
                    if track_data:
                        item['image_url'] = track_data.get('image_url')
                        print(f"  ✅ [{i+1}] {track_name} - imagem encontrada")
                except Exception as e:
                    print(f"  ❌ Erro no item {i+1}: {e}")
                    pass
        
        return jsonify({'success': True, 'data': days_with_ids})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/available_years')
def api_available_years():
    """Available years in data"""
    try:
        df_music = load_local_data()
        if df_music.empty:
            return jsonify({'success': True, 'years': []})
        
        years = sorted(df_music['ts'].dt.year.unique().tolist())
        return jsonify({'success': True, 'years': years})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ========== PLAYLIST CREATION API ==========

@app.route('/api/create_custom_playlist', methods=['POST'])
def api_create_custom_playlist():
    """Create custom playlist"""
    try:
        data = request.get_json()
        title = data.get('title', 'Custom Playlist')
        playlist_type = data.get('type', 'tracks')
        track_keys = data.get('track_keys', [])
        filters = data.get('filters', {})
        
        print(f"📝 Creating playlist '{title}' ({playlist_type}) with {len(track_keys)} tracks")
        
        # ✅ ADICIONAR: Verificar se tem Spotify client
        sp = get_spotify_client()
        if not sp:
            return jsonify({'success': False, 'error': 'Spotify not connected - please reconnect'}), 401
        
        # ✅ ADICIONAR: Obter user_id do Spotify
        try:
            user_id = sp.current_user()['id']
            print(f"🎵 Spotify user_id: {user_id}")
        except Exception as e:
            print(f"❌ Failed to get Spotify user: {e}")
            return jsonify({'success': False, 'error': 'Failed to authenticate with Spotify'}), 401
        
        # Search tracks on Spotify
        track_uris = search_tracks_for_playlist(track_keys)
        
        if not track_uris:
            return jsonify({'success': False, 'error': 'No tracks found on Spotify'})
        
        # Create playlist on Spotify
        try:
            playlist_description = f'Created from Spotify Pedro Dashboard - {playlist_type.upper()} analysis'
            if filters.get('year') != 'all' or filters.get('month') != 'all':
                playlist_description += f" (Filters: {filters})"
            
            print(f"Ã°Å¸Å½Âµ Creating playlist '{title}' for user {user_id}")
            
            playlist = sp.user_playlist_create(
                user=user_id,
                name=title, 
                public=False, 
                collaborative=False,
                description=playlist_description
            )
            
            print(f"Ã¢Å“â€¦ Playlist created: {playlist['id']}")
            
        except Exception as e:
            print(f"Ã¢ÂÅ’ Playlist creation error: {e}")
            return jsonify({'success': False, 'error': f'Failed to create playlist: {str(e)}'})
        
        # Add tracks in batches of 100
        try:
            batch_size = 100
            tracks_added = 0
            
            for i in range(0, len(track_uris), batch_size):
                batch = track_uris[i:i + batch_size]
                sp.playlist_add_items(playlist['id'], batch)
                tracks_added += len(batch)
                print(f"  Ã¢Å“â€¦ Added batch {i//batch_size + 1}: {len(batch)} tracks")
            
            print(f"Ã¢Å“â€¦ Total tracks added: {tracks_added}")
            
        except Exception as e:
            print(f"Ã¢ÂÅ’ Error adding tracks: {e}")
            return jsonify({'success': False, 'error': f'Playlist created but failed to add tracks: {str(e)}'})
        
        return jsonify({
            'success': True, 
            'url': playlist['external_urls']['spotify'],
            'tracks_added': tracks_added,
            'playlist_name': title,
            'playlist_id': playlist['id']
        })
        
    except Exception as e:
        print(f"Ã¢ÂÅ’ CRITICAL ERROR creating playlist: {e}")
        return jsonify({'success': False, 'error': f'Critical error: {str(e)}'})
    
@app.route('/api/play-track', methods=['POST'])
def api_play_track():
    """Play a track on user's active Spotify device"""
    try:
        data = request.get_json()
        track_uri = data.get('track_uri') or data.get('uri')
        
        if not track_uri:
            return jsonify({'success': False, 'error': 'No track URI provided'}), 400
        
        print(f"🎵 Playing track: {track_uri}")
        
        sp = get_spotify_client()
        if not sp:
            return jsonify({'success': False, 'error': 'Spotify not connected'}), 401
        
        # Get available devices
        devices = sp.devices()
        if not devices['devices']:
            return jsonify({
                'success': False, 
                'error': 'No active Spotify device found. Please open Spotify on any device.'
            }), 404
        
        # Play on first available device
        device_id = devices['devices'][0]['id']       
        sp.start_playback(device_id=device_id, uris=[track_uri])
        
        print(f"✅ Playing on device: {devices['devices'][0]['name']}")
        return jsonify({'success': True, 'message': 'Track playing'}), 200
        
    except Exception as e:
        print(f"❌ Error playing track: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/pause', methods=['POST'])
def api_pause():
    """Pause playback"""
    try:
        sp = get_spotify_client()
        if not sp:
            return jsonify({'success': False, 'error': 'Spotify not connected'}), 401
        
        sp.pause_playback()
        print("⏸️ Playback paused")
        return jsonify({'success': True, 'message': 'Playback paused'}), 200
        
    except Exception as e:
        print(f"❌ Error pausing: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/resume', methods=['POST'])
def api_resume():
    """Resume playback"""
    try:
        sp = get_spotify_client()
        if not sp:
            return jsonify({'success': False, 'error': 'Spotify not connected'}), 401
        
        sp.start_playback()
        print("▶️ Playback resumed")
        return jsonify({'success': True, 'message': 'Playback resumed'}), 200
        
    except Exception as e:
        print(f"❌ Error resuming: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/next', methods=['POST'])
def api_next():
    """Skip to next track"""
    try:
        sp = get_spotify_client()
        if not sp:
            return jsonify({'success': False, 'error': 'Spotify not connected'}), 401
        
        sp.next_track()
        print("⏭️ Skipped to next track")
        return jsonify({'success': True, 'message': 'Skipped to next'}), 200
        
    except Exception as e:
        print(f"❌ Error skipping: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/previous', methods=['POST'])
def api_previous():
    """Skip to previous track"""
    try:
        sp = get_spotify_client()
        if not sp:
            return jsonify({'success': False, 'error': 'Spotify not connected'}), 401
        
        sp.previous_track()
        print("⏮️ Skipped to previous track")
        return jsonify({'success': True, 'message': 'Skipped to previous'}), 200
        
    except Exception as e:
        print(f"❌ Error skipping: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    print("=" * 80)
    print("Ã°Å¸Å½Âµ SPOTIFY PEDRO - ADVANCED ANALYTICS DASHBOARD")
    print("=" * 80)
    print("Ã¢Å“â€¦ Features:")
    print("   Ã°Å¸â€œÅ  Advanced music listening analytics")
    print("   Ã°Å¸Å½Âµ Spotify API integration with automated search") 
    print("   Ã°Å¸Å½Â§ Professional music player with FORCED autoplay")
    print("   Ã°Å¸â€“Â¼Ã¯Â¸Â  High-quality images and metadata")
    print("   Ã°Å¸â€œË† Interactive data visualizations")
    print("   Ã°Å¸â€œâ€¦ Calendar view for individual tracks")
    print("   Ã°Å¸â€â€ž REPEAT SPIRALS: Max plays in single day")
    print("   Ã°Å¸â€œâ€  REPEAT DAYS: Max consecutive days listening")
    print("   Ã°Å¸Å½Âµ Custom playlist creation (TOP 50)")
    print("   Ã°Å¸â€œÂ± Responsive professional design")
    print("   Ã°Å¸Å½â€° Professional notifications and UX")
    print()
    print("Ã°Å¸â€ â€¢ Corrected analytics:")
    print("   Ã¢Å“â€¦ Repeat Spirals = MAX plays in ONE day")
    print("   Ã¢Å“â€¦ Repeat Days = MAX consecutive days")
    print("   Ã¢Å“â€¦ Calendar modal with full history (no filters)")
    print("   Ã¢Å“â€¦ FORCED autoplay system")
    print("   Ã¢Å“â€¦ Correct layout order")
    print()
    print("Ã°Å¸Å’Â URL: http://localhost:5000")
    print("Ã°Å¸â€â€˜ Connect once for full functionality")
    print("=" * 80)
    app.run(debug=False, port=5000)