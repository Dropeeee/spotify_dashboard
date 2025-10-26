# config.py
import os
from datetime import timedelta
from dotenv import load_dotenv

# ✅ CARREGAR .env ANTES DE TUDO
load_dotenv()

class Config:
    """Configuração centralizada"""
    
    # Spotify API (lê do .env)
    SPOTIFY_CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
    SPOTIFY_CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')
    REDIRECT_URI = os.environ.get('REDIRECT_URI', 'https://web-production-4121.up.railway.app/callback')
    
    # Flask
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-key-CHANGE')
    
    # Upload
    UPLOAD_FOLDER = os.path.join(os.getcwd(), 'user_uploads')
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB
    ALLOWED_EXTENSIONS = {'json'}
    
    # Session
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
