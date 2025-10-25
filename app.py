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
from data_processing import (
    load_streaming_history, 
    filter_music,
    top_tracks,
    top_artists, 
    top_albums,
    daily_history,
    repeat_spirals_optimized,
    viciado_tracks_top20
)
from config import Config

app = Flask(__name__)
app.config.from_object(Config)
app.secret_key = Config.SECRET_KEY

# ✅ ADICIONAR configuração de sessão:
from datetime import timedelta

# Configuração baseada no ambiente
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
    # NÃO criar user_id aqui! Criar só em /api/save-username



# Criar pasta de uploads
os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)


# Global cache for performance
app_cache = {}

# ============================================================================
# FUNÇÕES MULTI-USER
# ============================================================================

def allowed_file(filename):
    """Valida se ficheiro é .json"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'json'

def get_user_folder():
    """Retorna pasta única por utilizador"""
    if 'user_id' not in session:
        raise ValueError("❌ user_id não existe na sessão!")
    user_folder = os.path.join(Config.UPLOAD_FOLDER, session['user_id'])
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def load_user_data_from_files(user_folder):
    """Carrega dados de ficheiros JSON de uma pasta específica"""
    json_files = [f for f in os.listdir(user_folder) if f.endswith('.json')]
    
    if not json_files:
        raise FileNotFoundError(f"No JSON files in {user_folder}")
    
    print(f"📊 Loading {len(json_files)} JSON files from user folder...")
    all_data = []
    
    for json_file in json_files:
        filepath = os.path.join(user_folder, json_file)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
        except Exception as e:
            print(f"❌ Error loading {json_file}: {e}")
            continue
    
    if not all_data:
        raise ValueError("No data loaded from JSON files")
    
    # Converter para DataFrame (mesma estrutura que load_streaming_history())
    df = pd.DataFrame(all_data)
    df['ts'] = pd.to_datetime(df['ts'])
    if df['ts'].dt.tz is not None:
        df['ts'] = df['ts'].dt.tz_convert(None)
    
    print(f"✅ {len(df):,} records loaded from user files")
    return df

def get_redirect_uri():
    """Determinar redirect URI dinamicamente baseado no ambiente"""
    if request and request.url_root:
        base_url = request.url_root.rstrip('/')
        redirect_uri = f"{base_url}/callback"
        print(f"🔗 Using dynamic redirect URI: {redirect_uri}")
        return redirect_uri
    
    return Config.REDIRECT_URI  # fallback para desenvolvimento


def get_spotify_client():
    """Spotify client with all required scopes"""
    scope = 'user-top-read playlist-modify-public playlist-modify-private streaming user-read-private user-modify-playback-state user-read-playback-state'
    
    if 'user_id' in session and session.get('files_uploaded'):
        cache_path = os.path.join(get_user_folder(), '.spotify_cache')
    else:
        cache_path = '.cache-default'

    try:
        redirect_uri = get_redirect_uri()  # ✅ ADICIONA ESTA LINHA

        auth_manager = SpotifyOAuth(
            client_id=Config.SPOTIFY_CLIENT_ID,
            client_secret=Config.SPOTIFY_CLIENT_SECRET,
            redirect_uri=redirect_uri,  # ✅ USA A VARIÁVEL DINÂMICA

        )

        
        token_info = auth_manager.get_cached_token()
        if not token_info:
            return None
        
        if auth_manager.is_token_expired(token_info):
            token_info = auth_manager.refresh_access_token(token_info['refresh_token'])
        
        return spotipy.Spotify(auth=token_info['access_token'])
        
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return None
def load_local_data():
    """Load data ISOLADO por utilizador OU de path local"""
    
    # Multi-user mode (se tem user_id E ficheiros uploaded)
    if 'user_id' in session and session.get('files_uploaded', False):
        cache_key = f'df_music_{session["user_id"]}'
        
        if cache_key not in app_cache:
            try:
                user_folder = get_user_folder()
                
                # Verificar cache em disco
                cache_file = os.path.join(user_folder, 'processed_data.pkl')
                if os.path.exists(cache_file):
                    print(f"📊 Loading cached data for user {session['user_id'][:8]}...")
                    df_music = pd.read_pickle(cache_file)
                    app_cache[cache_key] = df_music
                    return df_music
                
                # Processar ficheiros uploaded
                print(f"📊 Processing uploaded files for user {session['user_id'][:8]}...")
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
    
    # Development mode (path hardcoded)
    else:
        if 'df_music_default' not in app_cache:
            try:
                print("📊 Loading from hardcoded path (development mode)")
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
            print(f"❌ API error: {e}")
            return []
    return []

def search_track_get_id(track_name, artist_name):
    sp = get_spotify_client()
    if not sp:
        print(f"❌ No Spotify client available")
        return None
    
    try:
        # BUSCA SIMPLES - Usa o Spotify search normal
        query = f'track:"{track_name}" artist:"{artist_name}"'
        print(f"🔍 Searching: {query}")
        
        results = sp.search(q=query, type='track', limit=5)
        
        if results['tracks']['items']:
            # PEGAR O PRIMEIRO ELEMENTO DO ARRAY
            track = results['tracks']['items'][0]
            
            print(f"   Found: {track['name']} - {track['artists'][0]['name']}")
            
            # Get image URL com acesso correcto ao array
            image_url = None
            if track['album']['images']:
                images = track['album']['images']
                # images é ARRAY, usa índice 1 ou 0
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
        
        print(f"   ❌ No results found")
        return None
    
    except Exception as e:
        print(f"❌ Search error for '{track_name}': {e}")
        import traceback
        traceback.print_exc()
        return None



def enhance_data_with_spotify_ids(data, data_type='track'):
    """Search Spotify IDs for all items"""
    sp = get_spotify_client()
    if not sp:
        return data
    
    enhanced_data = []
    
    print(f"🔍 Enhancing {len(data)} {data_type}s with Spotify IDs...")
    
    for i, item in enumerate(data):
        enhanced_item = item.copy()
        
        try:
            if data_type == 'track':
                track_name = item.get('enhanced_name') or item.get('track_key', '').split(' - ')[0] or 'Unknown'
                artist_name = item.get('enhanced_artist') or item.get('track_key', '').split(' - ')[1] if ' - ' in item.get('track_key', '') else 'Unknown'
                
                print(f"  [{i+1}/{len(data)}] Searching: {track_name} - {artist_name}")
                
                track_data = search_track_get_id(track_name, artist_name)
                if track_data:
                    enhanced_item.update(track_data)
                    print(f"    ✅ Enhanced with ID: {track_data['id']}")
                else:
                    print(f"    ❌ No ID found")
            
            elif data_type == 'artist':
                artist_name = item.get('enhanced_name') or item.get('artist_key', '')
                
                results = sp.search(q=f'artist:"{artist_name}"', type='artist', limit=1)
                if results['artists']['items']:
                    artist = results['artists']['items'][0]
                    enhanced_item['artist_id'] = artist['id']
                    enhanced_item['spotify_url'] = artist['external_urls']['spotify']
                    if artist['images']:
                        enhanced_item['image_url'] = artist['images'][1]['url'] if len(artist['images']) > 1 else artist['images'][0]['url']
            
            elif data_type == 'album':
                album_name = item.get('enhanced_name') or item.get('album_key', '')
                
                results = sp.search(q=f'album:"{album_name}"', type='album', limit=1)
                if results['albums']['items']:
                    album = results['albums']['items'][0]
                    enhanced_item['album_id'] = album['id']
                    enhanced_item['spotify_url'] = album['external_urls']['spotify']
                    if album['images']:
                        enhanced_item['image_url'] = album['images'][1]['url'] if len(album['images']) > 1 else album['images'][0]['url']
        
        except Exception as e:
            print(f"    ❌ Error enhancing {item}: {e}")
        
        enhanced_data.append(enhanced_item)
    
    # Count how many IDs we got
    ids_found = sum(1 for item in enhanced_data if item.get('id') or item.get('artist_id') or item.get('album_id'))
    print(f"✅ Enhanced {ids_found}/{len(data)} items with Spotify IDs")
    
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
        print("❌ Spotify client not available")
        return []
    
    found_tracks = []
    
    print(f"🔍 Searching {len(track_keys)} tracks on Spotify...")
    
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
                print(f"  ✅ [{i+1}/{len(track_keys)}] Found: {track_data['name']} - {track_data['artist']}")
            else:
                print(f"  ❌ [{i+1}/{len(track_keys)}] Not found: {track_key}")
                    
        except Exception as e:
            print(f"    ❌ Error searching {track_key}: {e}")
            continue
    
    print(f"✅ {len(found_tracks)} tracks found out of {len(track_keys)} requested")
    return found_tracks

# ========== CORRECT ANALYTICS FUNCTIONS ==========

def repeat_spirals_max_single_day(df, n=50, time_period='all'):
    """
    REPEAT SPIRALS: O número máximo de vezes que ouviste uma música NUM SÓ DIA/SEMANA/MÊS no período
    APENAS com plays INTENTIONAL (tu escolheste a música)

    Args:
        df: DataFrame filtrado
        n: Número de resultados
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

def consecutive_days_listening(df, n=50):
    """
    REPEAT DAYS: O número máximo de DIAS SEGUIDOS que ouviste uma música no período
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


def top_tracks_really_played(df, n=50):
    """
    TOP TRACKS com APENAS plays INTENTIONAL (REALLY PLAYED)
    Filtra apenas músicas que tu escolheste ouvir
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



def top_artists_really_played(df, n=50):
    """
    TOP ARTISTS com APENAS plays INTENTIONAL (REALLY PLAYED)
    Filtra apenas músicas que tu escolheste ouvir
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


def top_albums_really_played(df, n=50):
    """
    TOP ALBUMS com APENAS plays INTENTIONAL (REALLY PLAYED)
    Filtra apenas músicas que tu escolheste ouvir
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
    
    # ✅ CRIAR user_id AQUI (só quando user guarda username)
    if 'user_id' not in session:
        session['user_id'] = str(uuid.uuid4())
        print(f"\n🆔 NEW user_id created: {session['user_id']}")
    
    session['username'] = username
    session.modified = True
    
    print(f"✅ Username saved: {username}")
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
        print(f"✅ File uploaded: {filename}")
        
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
    
    print(f"✅ Upload complete: {len(json_files)} files")
    
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
    
    redirect_uri = get_redirect_uri()  # ✅ ADICIONA
    
    auth_manager = SpotifyOAuth(
        client_id=Config.SPOTIFY_CLIENT_ID,
        client_secret=Config.SPOTIFY_CLIENT_SECRET,
        redirect_uri=redirect_uri,  # ✅ USA DINÂMICO
        scope='user-top-read playlist-modify-public playlist-modify-private streaming user-read-private user-modify-playback-state user-read-playback-state',
        cache_path=cache_path,
        show_dialog=True
    )
    
    auth_url = auth_manager.get_authorize_url()
    return redirect(auth_url)

@app.route('/callback')
def callback():
    """OAuth callback - processa autenticação Spotify"""
    code = request.args.get('code')
    
    if not code:
        print("❌ No code in callback")
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
            print(f"📁 User folder: {user_folder}")
            print(f"📄 JSON files: {len(json_files)}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            cache_path = '.cache-default'
    else:
        cache_path = '.cache-default'
        print("📁 Using default cache")
    
    # OAuth
    redirect_uri = get_redirect_uri()  # ✅ ADICIONA
    
    auth_manager = SpotifyOAuth(
        client_id=Config.SPOTIFY_CLIENT_ID,
        client_secret=Config.SPOTIFY_CLIENT_SECRET,
        redirect_uri=redirect_uri,  # ✅ USA DINÂMICO
        scope='user-top-read playlist-modify-public playlist-modify-private streaming user-read-private user-modify-playback-state user-read-playback-state',
        cache_path=cache_path
    )
    
    try:
        token_info = auth_manager.get_access_token(code, as_dict=True, check_cache=False)
        
        if not token_info:
            raise ValueError("Failed to get token")
        
        print("✅ Spotify token obtained")
        session['spotify_authenticated'] = True
        
        # CARREGAR DADOS
        if session.get('files_uploaded'):
            print("\n📊 Loading uploaded files...")
            
            try:
                df_music = load_local_data()
                
                if not df_music.empty:
                    session['data_loaded'] = True
                    print(f"✅ {len(df_music):,} records loaded")
                else:
                    print("❌ DataFrame empty!")
                    
            except Exception as e:
                print(f"❌ Error: {e}")
                import traceback
                traceback.print_exc()
        else:
            # Dev mode
            print("\n📊 Dev mode...")
            df_music = load_local_data()
            if not df_music.empty:
                session['data_loaded'] = True
                print(f"✅ {len(df_music):,} records")
        
        session.modified = True
        
        print("\n" + "="*70)
        print("CALLBACK END")
        print(f"data_loaded: {session.get('data_loaded')}")
        print(f"spotify_authenticated: {session.get('spotify_authenticated')}")
        print("="*70 + "\n")
        
        response = app.make_response(redirect(url_for('home')))
        return response
        
    except Exception as e:
        print(f"\n❌ CALLBACK ERROR: {e}")
        import traceback
        traceback.print_exc()
        return redirect(url_for('home'))


@app.route('/logout')
def logout():
    """Limpa sessão e dados do utilizador"""
    if 'user_id' in session:
        user_folder = os.path.join(Config.UPLOAD_FOLDER, session['user_id'])
        if os.path.exists(user_folder):
            shutil.rmtree(user_folder)
    
    session.clear()
    return redirect(url_for('home'))


@app.route('/api/artist_top_tracks')
def get_artist_top_tracks():
    """Get top 10 tracks for a specific artist"""
    artist_name = request.args.get('artist_name', '').strip()  # ✅ STRIP aqui
    
    df = load_local_data()
    if df.empty:
        return jsonify({'success': False, 'error': 'No data available'})
    
    try:
        # ✅ STRIP na coluna do DataFrame também
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
                'name': track_name.strip(),  # ✅ STRIP
                'artist': artist.strip(),     # ✅ STRIP
                'plays': int(row['plays']),
                'image_url': None
            })
        
        # ✅ Enrich TODAS as 10 (não só top 5)
        print(f"🔍 Enriquecendo TODAS as 10 músicas com imagens para artist: {artist_name}")
        for i, item in enumerate(result):  # ✅ TODAS
            try:
                track_data = search_track_get_id(item['name'], item['artist'])
                if track_data:
                    item['image_url'] = track_data.get('image_url')
                    print(f"   ✅ [{i+1}] {item['name']} - imagem encontrada")
            except Exception as e:
                print(f"   ❌ Erro na track {i+1}: {e}")
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
    album_name = request.args.get('album_name', '').strip()  # ✅ STRIP aqui
    
    df = load_local_data()
    if df.empty:
        return jsonify({'success': False, 'error': 'No data available'})
    
    try:
        # ✅ STRIP na coluna do DataFrame também
        df_album = df[df['master_metadata_album_album_name'].str.strip() == album_name].copy()
        
        if df_album.empty:
            # Debug: Mostrar albums similares
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
                'name': track_name.strip(),  # ✅ STRIP
                'artist': artist.strip(),     # ✅ STRIP
                'plays': int(row['plays']),
                'image_url': None
            })
        
        # ✅ Enrich TODAS as 10 (não só top 5)
        print(f"🔍 Enriquecendo TODAS as 10 músicas com imagens para album: {album_name}")
        for i, item in enumerate(result):  # ✅ TODAS
            try:
                track_data = search_track_get_id(item['name'], item['artist'])
                if track_data:
                    item['image_url'] = track_data.get('image_url')
                    print(f"   ✅ [{i+1}] {item['name']} - imagem encontrada")
            except Exception as e:
                print(f"   ❌ Erro na track {i+1}: {e}")
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



@app.route('/')
def home():
    """Landing page ou dashboard"""
    
    # ✅ DEBUG
    print(f"\n[HOME] files_uploaded: {session.get('files_uploaded')}")
    print(f"[HOME] data_loaded: {session.get('data_loaded')}")
    print(f"[HOME] spotify_authenticated: {session.get('spotify_authenticated')}\n")
    
    if session.get('files_uploaded'):
        if session.get('data_loaded') and session.get('spotify_authenticated'):
            print("→ Rendering dashboard (multi-user mode)")
            return render_template('dashboard.html', username=session.get('username', 'User'))
        else:
            print("→ Rendering landing (need auth or data)")
            return render_template('landing.html')
    else:
        # Dev mode
        df = load_local_data()
        
        if not df.empty:
            sp = get_spotify_client()
            
            if sp:
                print("→ Rendering dashboard (dev mode)")
                return render_template('dashboard.html', username=session.get('username', 'User'))
            else:
                # HTML inline para dev
                redirect_uri = get_redirect_uri()  # Adicionar esta linha

                auth_manager = SpotifyOAuth(
                    client_id=Config.SPOTIFY_CLIENT_ID,
                    client_secret=Config.SPOTIFY_CLIENT_SECRET,
                    redirect_uri=redirect_uri,
                    scope = 'streaming user-read-private user-read-email user-top-read user-read-recently-played user-library-read playlist-modify-public playlist-modify-private user-modify-playback-state user-read-playback-state'
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
                        <h1>🎵 Spotify Dashboard</h1>
                        <p>Connect your Spotify account</p>
                        <a href="{auth_url}" class="btn">Connect Spotify</a>
                    </div>
                </body>
                </html>
                '''
        
        print("→ Rendering landing (no data)")
        return render_template('landing.html')



# ========== API SEARCH ENDPOINT ==========

@app.route('/api/search_track')
def api_search_track():
    """Search for specific track on Spotify and return ID"""
    track_name = request.args.get('track_name', '')
    artist_name = request.args.get('artist_name', '')
    
    if not track_name:
        return jsonify({'success': False, 'error': 'Track name required'})
    
    print(f"🔍 API Search request: {track_name} - {artist_name}")
    
    track_data = search_track_get_id(track_name, artist_name)
    
    if track_data:
        print(f"✅ API Search success: {track_data['name']} - {track_data['artist']} (ID: {track_data['id']})")
        return jsonify({
            'success': True,
            'track': track_data
        })
    else:
        print(f"❌ API Search failed: {track_name} - {artist_name}")
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
                'plays': int(plays)
            })

        # Search Spotify IDs for ALL tracks
        tracks_with_ids = enhance_data_with_spotify_ids(tracks_list, 'track')

        return jsonify({'success': True, 'data': tracks_with_ids})  # ✅ CORRETO
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
        alimit = int(request.args.get('limit', 10))  # Default 10
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
                'max_single_day_plays': max_single_day
            })

        # Search Spotify IDs for ALL tracks
        spirals_with_ids = enhance_data_with_spotify_ids(spirals_list, 'track')

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
                'consecutive_days': consecutive_days
            })
        
        # Search Spotify IDs for ALL tracks
        days_with_ids = enhance_data_with_spotify_ids(days_list, 'track')
        
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
        
        print(f"🎵 Creating playlist: '{title}' ({playlist_type}) with {len(track_keys)} tracks")
        
        # Validation
        if not track_keys:
            return jsonify({'success': False, 'error': 'No tracks provided'})
        
        # Get Spotify client
        sp = get_spotify_client()
        if not sp:
            return jsonify({'success': False, 'error': 'Spotify not connected - please reconnect'})
        
        # Test connection
        try:
            user_info = sp.current_user()
            user_id = user_info['id']
            print(f"✅ Connected as: {user_info.get('display_name', user_id)}")
        except Exception as e:
            print(f"❌ User info error: {e}")
            return jsonify({'success': False, 'error': f'Authentication error: {str(e)}'})
        
        # Search tracks on Spotify
        track_uris = search_tracks_for_playlist(track_keys)
        
        if not track_uris:
            return jsonify({'success': False, 'error': 'No tracks found on Spotify'})
        
        # Create playlist on Spotify
        try:
            playlist_description = f'Created from Spotify Pedro Dashboard - {playlist_type.upper()} analysis'
            if filters.get('year') != 'all' or filters.get('month') != 'all':
                playlist_description += f" (Filters: {filters})"
            
            print(f"🎵 Creating playlist '{title}' for user {user_id}")
            
            playlist = sp.user_playlist_create(
                user=user_id,
                name=title, 
                public=False, 
                collaborative=False,
                description=playlist_description
            )
            
            print(f"✅ Playlist created: {playlist['id']}")
            
        except Exception as e:
            print(f"❌ Playlist creation error: {e}")
            return jsonify({'success': False, 'error': f'Failed to create playlist: {str(e)}'})
        
        # Add tracks in batches of 100
        try:
            batch_size = 100
            tracks_added = 0
            
            for i in range(0, len(track_uris), batch_size):
                batch = track_uris[i:i + batch_size]
                sp.playlist_add_items(playlist['id'], batch)
                tracks_added += len(batch)
                print(f"  ✅ Added batch {i//batch_size + 1}: {len(batch)} tracks")
            
            print(f"✅ Total tracks added: {tracks_added}")
            
        except Exception as e:
            print(f"❌ Error adding tracks: {e}")
            return jsonify({'success': False, 'error': f'Playlist created but failed to add tracks: {str(e)}'})
        
        return jsonify({
            'success': True, 
            'url': playlist['external_urls']['spotify'],
            'tracks_added': tracks_added,
            'playlist_name': title,
            'playlist_id': playlist['id']
        })
        
    except Exception as e:
        print(f"❌ CRITICAL ERROR creating playlist: {e}")
        return jsonify({'success': False, 'error': f'Critical error: {str(e)}'})

if __name__ == '__main__':
    print("=" * 80)
    print("🎵 SPOTIFY PEDRO - ADVANCED ANALYTICS DASHBOARD")
    print("=" * 80)
    print("✅ Features:")
    print("   📊 Advanced music listening analytics")
    print("   🎵 Spotify API integration with automated search") 
    print("   🎧 Professional music player with FORCED autoplay")
    print("   🖼️  High-quality images and metadata")
    print("   📈 Interactive data visualizations")
    print("   📅 Calendar view for individual tracks")
    print("   🔄 REPEAT SPIRALS: Max plays in single day")
    print("   📆 REPEAT DAYS: Max consecutive days listening")
    print("   🎵 Custom playlist creation (TOP 50)")
    print("   📱 Responsive professional design")
    print("   🎉 Professional notifications and UX")
    print()
    print("🆕 Corrected analytics:")
    print("   ✅ Repeat Spirals = MAX plays in ONE day")
    print("   ✅ Repeat Days = MAX consecutive days")
    print("   ✅ Calendar modal with full history (no filters)")
    print("   ✅ FORCED autoplay system")
    print("   ✅ Correct layout order")
    print()
    print("🌐 URL: http://localhost:5000")
    print("🔑 Connect once for full functionality")
    print("=" * 80)
    app.run(debug=False, port=5000)