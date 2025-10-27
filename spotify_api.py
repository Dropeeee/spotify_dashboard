
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import base64
import requests
import time
import re
from urllib.parse import quote
import logging

logger = logging.getLogger(__name__)

class SpotifyEnhancer:
    def __init__(self, client_id=None, client_secret=None):
        """
        Para obter credenciais: https://developer.spotify.com/dashboard/applications
        """
        self.sp = None
        self.api_available = False

        if client_id and client_secret:
            try:
                self.sp = spotipy.Spotify(
                    client_credentials_manager=SpotifyClientCredentials(
                        client_id=client_id,
                        client_secret=client_secret
                    )
                )
                # Testar conexão
                self.sp.search('test', limit=1)
                self.api_available = True
                logger.info("✅ Spotify API conectada!")
            except Exception as e:
                logger.error(f"⚠️ Spotify API falhou: {e}")
                self.api_available = False
        else:
            logger.info("ℹ️ Spotify API não configurada - usando modo offline")

    def search_track_metadata(self, track_name, artist_name):
        if not self.api_available:
            return self._create_fallback_metadata(track_name, artist_name)

        try:
            clean_track = self._clean_search_term(track_name)
            clean_artist = self._clean_search_term(artist_name)

            query = f'track:"{clean_track}" artist:"{clean_artist}"'
            results = self.sp.search(q=query, type='track', limit=1)

            if results['tracks']['items']:
                track = results['tracks']['items'][0]
                return {
                    'name': track['name'],
                    'artist': track['artists'][0]['name'],
                    'image_url': track['album']['images'][0]['url'] if track['album']['images'] else None,
                    'spotify_url': track['external_urls']['spotify'],
                    'preview_url': track['preview_url'],
                    'popularity': track['popularity'],
                    'uri': track['uri']
                }
            else:
                return self._create_fallback_metadata(track_name, artist_name)

        except Exception as e:
            logger.error(f"Erro na busca: {e}")
            return self._create_fallback_metadata(track_name, artist_name)

    def search_artist_metadata(self, artist_name):
        if not self.api_available:
            return self._create_fallback_artist_metadata(artist_name)

        try:
            results = self.sp.search(q=f'artist:"{artist_name}"', type='artist', limit=1)

            if results['artists']['items']:
                artist = results['artists']['items'][0]
                return {
                    'name': artist['name'],
                    'image_url': artist['images'][0]['url'] if artist['images'] else None,
                    'spotify_url': artist['external_urls']['spotify'],
                    'followers': artist['followers']['total'],
                    'genres': artist['genres'],
                    'uri': artist['uri']
                }
            else:
                return self._create_fallback_artist_metadata(artist_name)

        except Exception as e:
            logger.error(f"Erro na busca de artista: {e}")
            return self._create_fallback_artist_metadata(artist_name)

    def search_album_metadata(self, album_name, artist_name=""):
        """Search for album metadata with VALIDATION"""
        if not self.api_available or not self.sp:
            return None
        
        try:
            album_clean = album_name.strip()
            artist_clean = artist_name.strip() if artist_name else ""
            
            # Múltiplas queries para garantir que encontra
            if artist_clean:
                queries = [
                    f'album:"{album_clean}" artist:"{artist_clean}"',
                    f'album:{album_clean} artist:{artist_clean}',
                    f'{album_clean} {artist_clean}'
                ]
            else:
                queries = [
                    f'album:"{album_clean}"',
                    f'{album_clean}'
                ]
            
            best_match = None
            best_score = 0
            
            for query in queries:
                try:
                    results = self.sp.search(q=query, type='album', limit=10)
                    
                    if not results['albums']['items']:
                        continue
                    
                    for album in results['albums']['items']:
                        result_name = album['name'].lower().strip()
                        result_artist = album['artists'][0]['name'].lower().strip()  # ✅ [0] AQUI
                        
                        # Scoring system
                        score = 0
                        
                        # Album name score
                        if result_name == album_clean.lower():
                            score += 100
                        elif album_clean.lower() in result_name or result_name in album_clean.lower():
                            score += 70
                        else:
                            album_words = set(album_clean.lower().split())
                            result_words = set(result_name.split())
                            common = album_words & result_words
                            score += len(common) * 10
                        
                        # Artist score (if provided)
                        if artist_clean:
                            if result_artist == artist_clean.lower():
                                score += 100
                            elif artist_clean.lower() in result_artist or result_artist in artist_clean.lower():
                                score += 70
                            else:
                                artist_words = set(artist_clean.lower().split())
                                result_artist_words = set(result_artist.split())
                                common = artist_words & result_artist_words
                                score += len(common) * 10
                        
                        # Keep best match
                        if score > best_score:
                            best_score = score
                            best_match = album
                    
                    # Stop if found good match
                    if best_score >= 100:
                        break
                        
                except Exception as e:
                    continue
            
            if best_match:
                image_url = None
                if best_match['images']:
                    images = best_match['images']
                    image_url = images[1]['url'] if len(images) > 1 else images[0]['url']  # ✅ [1] e [0] AQUI
                
                return {
                    'id': best_match['id'],
                    'name': best_match['name'],
                    'artist': best_match['artists'][0]['name'],  # ✅ [0] AQUI
                    'spotify_url': best_match['external_urls']['spotify'],
                    'image_url': image_url,
                    'release_date': best_match.get('release_date', ''),
                    'match_score': best_score
                }
            
            return None
        
        except Exception as e:
            print(f"❌ Album search error: {e}")
            return None

    def _clean_search_term(self, term):
        if not term:
            return ""
        clean = re.sub(r'[^\w\s-]', ' ', str(term))
        clean = re.sub(r'\s+', ' ', clean).strip()
        return clean

    def _create_fallback_metadata(self, track_name, artist_name):
        return {
            'name': track_name or 'Unknown Track',
            'artist': artist_name or 'Unknown Artist',
            'image_url': 'https://via.placeholder.com/300x300/1ED760/FFFFFF?text=♪',
            'spotify_url': f'https://open.spotify.com/search/{quote(f"{track_name} {artist_name}")}',
            'preview_url': None,
            'popularity': 0,
            'uri': f'spotify:track:unknown'
        }

    def _create_fallback_artist_metadata(self, artist_name):
        return {
            'name': artist_name or 'Unknown Artist',
            'image_url': 'https://via.placeholder.com/300x300/1ED760/FFFFFF?text=🎤',
            'spotify_url': f'https://open.spotify.com/search/{quote(artist_name or "")}',
            'followers': 0,
            'genres': [],
            'uri': f'spotify:artist:unknown'
        }

    def _create_fallback_album_metadata(self, album_name, artist_name):
        return {
            'name': album_name or 'Unknown Album',
            'artist': artist_name or 'Unknown Artist',
            'image_url': 'https://via.placeholder.com/300x300/1ED760/FFFFFF?text=💿',
            'spotify_url': f'https://open.spotify.com/search/{quote(f"{album_name} {artist_name}")}',
            'release_date': 'Unknown',
            'uri': f'spotify:album:unknown'
        }

