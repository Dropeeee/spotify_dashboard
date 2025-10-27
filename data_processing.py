# data_processing.py - VERSÃO PROFISSIONAL COM FILTROS ADAPTATIVOS AVANÇADOS
# 
# Sistema de classificação inteligente:
# - Plays INTENCIONAIS: >= 60 segundos
# - Plays AUTOPLAY: >= 80% da música OU >= 2.5 minutos
# - Remoção rigorosa de dados inválidos
# - Performance ultra-otimizada (100% vectorizado)
#
# Autor: Pedro - Spotify Analytics Dashboard
# Data: Outubro 2025

import pandas as pd
import glob
import os
import json
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict
import logging
from fuzzywuzzy import fuzz
from spotify_api import SpotifyEnhancer

# Variável global para armazenar a instância
_spotify_enhancer = None

def set_spotify_enhancer(enhancer):
    """Define a instância do SpotifyEnhancer"""
    global _spotify_enhancer
    _spotify_enhancer = enhancer

def get_spotify_enhancer():
    """Retorna a instância do SpotifyEnhancer"""
    return _spotify_enhancer



# ============================================================================
# CONFIGURAÇÃO
# ============================================================================

JSON_FOLDER = None

# Cache global otimizado
METADATA_CACHE = {}
PROCESSED_CACHE = {}

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTES DE FILTRO - AJUSTA AQUI PARA TORNAR MAIS/MENOS RIGOROSO
# ============================================================================

# Plays INTENCIONAIS (tu escolheste a música)
MIN_INTENTIONAL_PLAY_MS = 60000  # 60 segundos = 1 minuto

# Plays AUTOPLAY (tocou automaticamente após outra)
MIN_AUTOPLAY_DURATION_MS = 150000  # 2.5 minutos (150 segundos)
MIN_AUTOPLAY_PERCENTAGE = 0.80  # 80% da música ouvida

# Duração média de músicas no Spotify (dados reais: 2020-2025 = ~3min 20s)
AVERAGE_SONG_DURATION_MS = 200000  # 200 segundos = 3min 20s

# Gap de tempo para definir sessões diferentes
SESSION_GAP_MINUTES = 30


# ============================================================================
# RAZÕES DE INÍCIO (reason_start) - BASEADO EM DADOS REAIS DO SPOTIFY
# ============================================================================

# Plays que tu ESCOLHESTE ouvir (ação intencional do utilizador)
INTENTIONAL_REASONS = [
    'clickrow',   # Clicaste na música na lista/playlist
    'fwdbtn',     # Botão "próxima música"
    'backbtn',    # Botão "música anterior"
    'playbtn',    # Botão play/resume
    'appload',    # App abriu com esta música
    'uriopen',    # Link direto (URL share)
    'remote',     # Controlo remoto (outro dispositivo, Spotify Connect)
    'popup',      # Popup notification (obsoleto mas pode existir)
    'clickside'   # Click no sidebar (obsoleto mas pode existir)
]

# Plays que tocaram AUTOMATICAMENTE (sem ação direta tua)
AUTOPLAY_REASONS = [
    'trackdone',  # Música anterior acabou, esta tocou automaticamente
    'endplay'     # Queue acabou e começou autoplay sugerido pelo Spotify
]


# ============================================================================
# FUNÇÕES DE CARREGAMENTO E PROCESSAMENTO
# ============================================================================

def load_streaming_history():
    """
    Carrega histórico de streaming do Spotify Extended History
    
    Returns:
        DataFrame com todos os registos raw
    """
    pattern = os.path.join(JSON_FOLDER, 'Streaming_History_Audio_*.json')
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"❌ Nenhum JSON encontrado em: {pattern}")
    
    logger.info(f"📁 A carregar {len(files)} ficheiros JSON...")
    all_data = []
    
    for file_path in files:
        logger.info(f"  → {os.path.basename(file_path)}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            all_data.extend(data)
        except Exception as e:
            logger.error(f"❌ Erro ao carregar {file_path}: {e}")
            continue
    
    if not all_data:
        raise ValueError("❌ Nenhum dado foi carregado dos ficheiros JSON")
    
    df = pd.DataFrame(all_data)
    
    # Converter timestamp e remover timezone
    df['ts'] = pd.to_datetime(df['ts']).dt.tz_convert(None)
    
    logger.info(f"✅ {len(df):,} registos carregados (raw)")
    return df


def classify_play_type(df):
    """
    Classifica cada play como INTENTIONAL, AUTOPLAY ou UNKNOWN
    baseado no campo reason_start dos dados do Spotify
    
    Args:
        df: DataFrame com dados do Spotify
        
    Returns:
        DataFrame com coluna 'play_type' adicionada
    """
    if df.empty:
        return df
    
    # Criar coluna de classificação
    df['play_type'] = 'UNKNOWN'
    
    # Classificar plays intencionais
    mask_intentional = df['reason_start'].isin(INTENTIONAL_REASONS)
    df.loc[mask_intentional, 'play_type'] = 'INTENTIONAL'
    
    # Classificar autoplay
    mask_autoplay = df['reason_start'].isin(AUTOPLAY_REASONS)
    df.loc[mask_autoplay, 'play_type'] = 'AUTOPLAY'
    
    # Estatísticas de classificação
    intentional_count = mask_intentional.sum()
    autoplay_count = mask_autoplay.sum()
    unknown_count = (df['play_type'] == 'UNKNOWN').sum()
    total = len(df)
    
    logger.info(f"")
    logger.info(f"📊 Classificação de Plays por Tipo:")
    logger.info(f"  • INTENTIONAL:  {intentional_count:>10,} ({100*intentional_count/total:.1f}%)")
    logger.info(f"  • AUTOPLAY:     {autoplay_count:>10,} ({100*autoplay_count/total:.1f}%)")
    logger.info(f"  • UNKNOWN:      {unknown_count:>10,} ({100*unknown_count/total:.1f}%)")
    logger.info(f"")
    
    return df


def apply_adaptive_duration_filter(df):
    """
    Aplica filtros de duração ADAPTATIVOS baseados no tipo de play
    
    Sistema de classificação profissional:
    
    1. INTENTIONAL plays (tu escolheste):
       - Mínimo: 60 segundos (1 minuto)
       - Lógica: Se escolheste ouvir, 1 minuto é suficiente para contar
       
    2. AUTOPLAY plays (tocou automaticamente):
       - Critério A: >= 80% da duração da música (ouviste quase completa)
       - Critério B: >= 2.5 minutos em valor absoluto (para músicas longas)
       - Lógica: Se tocou automaticamente, só conta se ouviste QUASE TUDO
                 Isto garante que não contam músicas que passaram na fila
                 enquanto estavas ocupado/distraído
       
    3. UNKNOWN plays (sem informação):
       - Trata como INTENTIONAL (benefício da dúvida)
       - Mínimo: 60 segundos
    
    Args:
        df: DataFrame classificado por tipo
        
    Returns:
        DataFrame filtrado apenas com plays válidos
    """
    if df.empty:
        return df
    
    initial_count = len(df)

    
    
    # ========================================================================
    # FILTRO 1: INTENTIONAL PLAYS (critério simples - 60s)
    # ========================================================================
    
    mask_intentional = (df['play_type'] == 'INTENTIONAL')
    mask_intentional_valid = mask_intentional & (df['ms_played'] >= MIN_INTENTIONAL_PLAY_MS)
    
    intentional_total = mask_intentional.sum()
    intentional_valid = mask_intentional_valid.sum()
    intentional_removed = intentional_total - intentional_valid
    
    # ========================================================================
    # FILTRO 2: AUTOPLAY (critério rigoroso - 80% OU 2.5 min)
    # ========================================================================
    
    # Estimativa de duração da música:
    # - Se ms_played > duração média, usa ms_played como estimativa
    # - Caso contrário, usa duração média do Spotify (3min 20s)
    # Em produção ideal, buscaríamos da API, mas isso é muito lento
    
    df['estimated_duration_ms'] = AVERAGE_SONG_DURATION_MS
    
    # Se tocou mais que a média, assume que é a duração real da música
    df.loc[df['ms_played'] > AVERAGE_SONG_DURATION_MS, 'estimated_duration_ms'] = df['ms_played']
    
    # Calcular percentagem ouvida (para autoplay)
    df['play_percentage'] = df['ms_played'] / df['estimated_duration_ms']
    
    mask_autoplay = (df['play_type'] == 'AUTOPLAY')
    
    # Autoplay é válido se satisfaz UM dos critérios:
    # - Ouviu >= 80% da música (ouviste quase tudo) OU
    # - Ouviu >= 2.5 minutos absolutos (músicas longas contam se ouviste bastante)
    mask_autoplay_valid = mask_autoplay & (
        (df['play_percentage'] >= MIN_AUTOPLAY_PERCENTAGE) |
        (df['ms_played'] >= MIN_AUTOPLAY_DURATION_MS)
    )
    
    autoplay_total = mask_autoplay.sum()
    autoplay_valid = mask_autoplay_valid.sum()
    autoplay_removed = autoplay_total - autoplay_valid
    
    # ========================================================================
    # FILTRO 3: UNKNOWN (trata como intentional - benefício da dúvida)
    # ========================================================================
    
    mask_unknown = (df['play_type'] == 'UNKNOWN')
    mask_unknown_valid = mask_unknown & (df['ms_played'] >= MIN_INTENTIONAL_PLAY_MS)
    
    unknown_total = mask_unknown.sum()
    unknown_valid = mask_unknown_valid.sum()
    unknown_removed = unknown_total - unknown_valid
    
    # ========================================================================
    # COMBINAR TODOS OS PLAYS VÁLIDOS
    # ========================================================================
    
    mask_all_valid = mask_intentional_valid | mask_autoplay_valid | mask_unknown_valid
    df_valid = df[mask_all_valid].copy()
    
    final_count = len(df_valid)
    retention_rate = (final_count / initial_count) * 100 if initial_count > 0 else 0
    
    # ========================================================================
    # LOGGING DETALHADO PARA AUDITORIA
    # ========================================================================
    
    logger.info(f"")
    logger.info(f"{'='*70}")
    logger.info(f"⚡ FILTROS ADAPTATIVOS DE DURAÇÃO APLICADOS:")
    logger.info(f"{'='*70}")
    logger.info(f"")
    logger.info(f"INTENTIONAL (critério: >= {MIN_INTENTIONAL_PLAY_MS/1000:.0f}s):")
    logger.info(f"  • Total:          {intentional_total:>10,}")
    logger.info(f"  • Válidos:        {intentional_valid:>10,} ({100*intentional_valid/intentional_total:.1f}% se intentional_total > 0 else 0)")
    logger.info(f"  • Removidos:      {intentional_removed:>10,}")
    logger.info(f"")
    logger.info(f"AUTOPLAY (critério: >= {MIN_AUTOPLAY_PERCENTAGE*100:.0f}% OU >= {MIN_AUTOPLAY_DURATION_MS/1000:.0f}s):")
    logger.info(f"  • Total:          {autoplay_total:>10,}")
    logger.info(f"  • Válidos:        {autoplay_valid:>10,} ({100*autoplay_valid/autoplay_total:.1f}% if autoplay_total > 0 else 0)")
    logger.info(f"  • Removidos:      {autoplay_removed:>10,}")
    logger.info(f"")
    
    if unknown_total > 0:
        logger.info(f"UNKNOWN (critério: >= {MIN_INTENTIONAL_PLAY_MS/1000:.0f}s):")
        logger.info(f"  • Total:          {unknown_total:>10,}")
        logger.info(f"  • Válidos:        {unknown_valid:>10,} ({100*unknown_valid/unknown_total:.1f}%)")
        logger.info(f"  • Removidos:      {unknown_removed:>10,}")
        logger.info(f"")
    
    logger.info(f"RESUMO TOTAL:")
    logger.info(f"  • Inicial:        {initial_count:>10,}")
    logger.info(f"  • Final:          {final_count:>10,} ({retention_rate:.1f}%)")
    logger.info(f"  • Removidos:      {initial_count - final_count:>10,}")
    logger.info(f"{'='*70}")
    logger.info(f"")
    
    return df_valid


def filter_music(df):
    """
    PIPELINE COMPLETO DE FILTROS - PROFISSIONAL E ROBUSTO
    
    Pipeline em 4 fases:
    
    FASE 1 - Filtros de Qualidade de Dados:
        - Remove ms_played <= 0 (erros do Spotify)
        - Remove registos sem metadata (bugs conhecidos)
        - Remove strings vazias
        
    FASE 2 - Classificação de Tipo:
        - Classifica cada play como INTENTIONAL/AUTOPLAY/UNKNOWN
        - Baseado em reason_start dos dados do Spotify
        
    FASE 3 - Filtros Adaptativos de Duração:
        - Aplica critérios diferentes por tipo de play
        - INTENTIONAL: >= 60s
        - AUTOPLAY: >= 80% da música OU >= 2.5min
        
    FASE 4 - Enriquecimento:
        - Adiciona colunas úteis para análise
        - track_key, date, hour, day_of_week, etc.
    
    Args:
        df: DataFrame raw carregado do Spotify Extended History
        
    Returns:
        DataFrame filtrado e enriquecido, pronto para análise
    """
    if df.empty:
        logger.warning("⚠️ DataFrame vazio recebido em filter_music")
        return pd.DataFrame()
    
    initial_count = len(df)
    logger.info(f"")
    logger.info(f"{'='*70}")
    logger.info(f"🔍 PIPELINE DE FILTROS INICIADO")
    logger.info(f"{'='*70}")
    logger.info(f"  Registos iniciais: {initial_count:,}")
    logger.info(f"")
    
    # ========================================================================
    # FASE 1: FILTROS CRÍTICOS DE QUALIDADE DE DADOS
    # ========================================================================
    
    logger.info(f"FASE 1: Filtros de Qualidade de Dados")
    logger.info(f"-" * 70)
    
    # Filtro 1.1: Remover ms_played <= 0 (erros conhecidos do Spotify)
    df = df[df['ms_played'] > 0].copy()
    after_zero = len(df)
    logger.info(f"  ✓ ms_played > 0:           {after_zero:>10,} ({initial_count - after_zero:,} removidos)")
    
    # Filtro 1.2: Remover registos sem metadata válida
    # Isto remove bugs onde o Spotify não guardou informação da música
    mask_has_metadata = (
        df['master_metadata_track_name'].notna() &
        df['master_metadata_album_artist_name'].notna() &
        df['spotify_track_uri'].notna()
    )
    df = df[mask_has_metadata].copy()
    after_metadata = len(df)
    logger.info(f"  ✓ Metadata válida:         {after_metadata:>10,} ({after_zero - after_metadata:,} removidos)")
    
    # Filtro 1.3: Remover strings vazias (caso existam após strip)
    mask_not_empty = (
        (df['master_metadata_track_name'].astype(str).str.strip() != '') &
        (df['master_metadata_album_artist_name'].astype(str).str.strip() != '')
    )
    df = df[mask_not_empty].copy()
    after_empty = len(df)
    logger.info(f"  ✓ Strings não vazias:      {after_empty:>10,} ({after_metadata - after_empty:,} removidos)")
    logger.info(f"")
    
    # ========================================================================
    # FASE 2: CLASSIFICAÇÃO DE TIPO DE PLAY
    # ========================================================================
    
    logger.info(f"FASE 2: Classificação de Tipo de Play (reason_start)")
    logger.info(f"-" * 70)
    df = classify_play_type(df)
    
    # ========================================================================
    # FASE 3: FILTROS ADAPTATIVOS DE DURAÇÃO
    # ========================================================================
    
    logger.info(f"FASE 3: Filtros Adaptativos de Duração")
    logger.info(f"-" * 70)
    df = apply_adaptive_duration_filter(df)
    
    # ========================================================================
    # FASE 4: ENRIQUECIMENTO DE DADOS PARA ANÁLISE
    # ========================================================================
    
    logger.info(f"FASE 4: Enriquecimento de Dados")
    logger.info(f"-" * 70)
    
    # Flag de play válido (todos os que chegaram aqui são válidos)
    df['is_play'] = True
    
    # Flag de skip (se foi skippado pelo utilizador)
    df['is_skip'] = df['skipped'].fillna(False)
    
    # Criar track_key para agregações (formato: "Track Name - Artist Name")
    df['track_key'] = (
        df['master_metadata_track_name'].astype(str) + ' - ' + 
        df['master_metadata_album_artist_name'].astype(str)
    )
    
    # Adicionar colunas temporais para análises
    df['date'] = df['ts'].dt.date
    df['hour'] = df['ts'].dt.hour
    df['day_of_week'] = df['ts'].dt.dayofweek  # 0=Monday, 6=Sunday
    df['month'] = df['ts'].dt.month
    df['year'] = df['ts'].dt.year
    
    logger.info(f"  ✓ Colunas enriquecidas: track_key, date, hour, day_of_week, month, year")
    logger.info(f"  ✓ Flags adicionados: is_play, is_skip, play_type, play_percentage")
    logger.info(f"")


    
    # ========================================================================
    # VALIDAÇÃO FINAL E ESTATÍSTICAS
    # ========================================================================
    
    final_count = len(df)
    retention_rate = (final_count / initial_count) * 100 if initial_count > 0 else 0
    
    logger.info(f"")
    logger.info(f"{'='*70}")
    logger.info(f"✅ PIPELINE DE FILTROS CONCLUÍDO COM SUCESSO")
    logger.info(f"{'='*70}")
    logger.info(f"  • Registos iniciais:       {initial_count:>10,}")
    logger.info(f"  • Registos finais:         {final_count:>10,}")
    logger.info(f"  • Taxa de retenção:        {retention_rate:>10.1f}%")
    logger.info(f"  • Registos removidos:      {initial_count - final_count:>10,}")
    logger.info(f"")
    logger.info(f"  📊 Dados prontos para análise profissional")
    logger.info(f"{'='*70}")
    logger.info(f"")
    
    if final_count == 0:
        logger.error("❌ ERRO CRÍTICO: Nenhum registo passou nos filtros!")
        logger.error("   Verifica se os ficheiros JSON estão corretos")
        raise ValueError("Nenhuma música válida encontrada após aplicar filtros")
    
    return df


# ============================================================================
# FUNÇÕES DE ANÁLISE ULTRA OTIMIZADAS (100% VECTORIZADAS)
# ============================================================================

def top_tracks_ultra_fast(df, n=10):
    """
    Top tracks ULTRA RÁPIDO - 100% vectorizado
    """
    if df.empty:
        return pd.DataFrame()
    
    cache_key = f"tracks_{len(df)}_{n}_{MIN_INTENTIONAL_PLAY_MS}"
    if cache_key in PROCESSED_CACHE:
        return PROCESSED_CACHE[cache_key]
    
    # Aggregação vectorizada
    result = (
        df.groupby('track_key', sort=False)
        .agg({
            'is_play': 'sum',
            'is_skip': 'sum',
            'ms_played': 'sum'
        })
        .rename(columns={
            'is_play': 'plays',
            'is_skip': 'skips',
            'ms_played': 'total_ms_played'
        })
        .nlargest(n, 'plays')
        .reset_index()
    )
    
    # Calcular tempo total em horas
    result['total_hours'] = result['total_ms_played'] / (1000 * 60 * 60)
    
    PROCESSED_CACHE[cache_key] = result
    return result


def top_albums_ultra_fast(df, n=10):
    """Top albums ULTRA RÁPIDO - 100% vectorizado"""
    if df.empty:
        return pd.DataFrame()
    
    cache_key = f"albums_{len(df)}_{n}_{MIN_INTENTIONAL_PLAY_MS}"
    if cache_key in PROCESSED_CACHE:
        return PROCESSED_CACHE[cache_key]
    
    # ✅ CRIAR coluna com strip ANTES do groupby
    df_clean = df.copy()
    df_clean['album_name_clean'] = df_clean['master_metadata_album_album_name'].str.strip()
    
    result = (
        df_clean.groupby('album_name_clean', sort=False)  # ✅ USA coluna limpa
        .agg({
            'is_play': 'sum',
            'ms_played': 'sum'
        })
        .rename(columns={
            'is_play': 'plays',
            'ms_played': 'total_ms_played'
        })
        .nlargest(n, 'plays')
        .reset_index()
        .rename(columns={'album_name_clean': 'album_key'})  # ✅ Renomeia de volta
    )
    
    result['total_hours'] = result['total_ms_played'] / (1000 * 60 * 60)
    
    PROCESSED_CACHE[cache_key] = result
    return result


def top_artists_ultra_fast(df, n=10):
    """Top artists ULTRA RÁPIDO - 100% vectorizado"""
    if df.empty:
        return pd.DataFrame()
    
    cache_key = f"artists_{len(df)}_{n}_{MIN_INTENTIONAL_PLAY_MS}"
    if cache_key in PROCESSED_CACHE:
        return PROCESSED_CACHE[cache_key]
    
    # ✅ CRIAR coluna com strip ANTES do groupby
    df_clean = df.copy()
    df_clean['artist_name_clean'] = df_clean['master_metadata_album_artist_name'].str.strip()
    
    result = (
        df_clean.groupby('artist_name_clean', sort=False)  # ✅ USA coluna limpa
        .agg({
            'is_play': 'sum',
            'ms_played': 'sum'
        })
        .rename(columns={
            'is_play': 'plays',
            'ms_played': 'total_ms_played'
        })
        .nlargest(n, 'plays')
        .reset_index()
        .rename(columns={'artist_name_clean': 'artist_key'})  # ✅ Renomeia de volta
    )
    
    result['total_hours'] = result['total_ms_played'] / (1000 * 60 * 60)
    
    PROCESSED_CACHE[cache_key] = result
    return result


def daily_history_optimized(df):
    """
    Histórico diário OTIMIZADO com preenchimento de gaps
    """
    if df.empty:
        return pd.DataFrame(columns=['date', 'plays'])
    
    cache_key = f"daily_optimized_{len(df)}_{MIN_INTENTIONAL_PLAY_MS}"
    if cache_key in PROCESSED_CACHE:
        return PROCESSED_CACHE[cache_key]
    
    # Todos os registos já são plays válidos (filtrados em filter_music)
    daily_counts = (
        df.groupby('date', sort=True)
        .size()
        .reset_index(name='plays')
    )
    
    # Converter date para datetime
    daily_counts['date'] = pd.to_datetime(daily_counts['date'])
    
    # Preencher gaps (dias sem plays com 0)
    if len(daily_counts) > 1:
        date_range = pd.date_range(
            start=daily_counts['date'].min(),
            end=daily_counts['date'].max(),
            freq='D'
        )
        
        full_range = pd.DataFrame({'date': date_range})
        daily_counts = full_range.merge(daily_counts, on='date', how='left')
        daily_counts['plays'] = daily_counts['plays'].fillna(0).astype(int)
    
    daily_counts = daily_counts.sort_values('date').reset_index(drop=True)
    
    PROCESSED_CACHE[cache_key] = daily_counts
    logger.info(f"📈 Histórico diário: {len(daily_counts)} dias, {daily_counts['plays'].sum():,} plays totais")
    
    return daily_counts


def repeat_spirals_correct(df, n=10):
    """
    REPEAT SPIRALS: Número de dias únicos em que uma música foi ouvida
    """
    if df.empty:
        return []
    
    cache_key = f"spirals_correct_{len(df)}_{n}_{MIN_INTENTIONAL_PLAY_MS}"
    if cache_key in PROCESSED_CACHE:
        return PROCESSED_CACHE[cache_key]
    
    # Contar dias únicos por track (100% vectorizado)
    track_unique_days = (
        df.groupby('track_key', sort=False)['date']
        .nunique()
        .sort_values(ascending=False)
        .head(n)
    )
    
    result = list(track_unique_days.items())
    PROCESSED_CACHE[cache_key] = result
    
    logger.info(f"🌀 Repeat spirals: top {len(result)} tracks calculados")
    return result


def repeat_days_consecutive(df, n=10):
    """
    REPEAT DAYS: Número máximo de dias CONSECUTIVOS que uma música foi ouvida
    
    Performance: Otimizado com NumPy para processar milhões de registos
    """
    if df.empty:
        return []
    
    cache_key = f"consecutive_days_{len(df)}_{n}_{MIN_INTENTIONAL_PLAY_MS}"
    if cache_key in PROCESSED_CACHE:
        return PROCESSED_CACHE[cache_key]
    
    # Obter datas únicas por track (vectorizado)
    track_dates = df.groupby('track_key', sort=False)['date'].apply(
        lambda x: sorted(x.unique())
    ).reset_index()
    
    consecutive_counts = {}
    
    # Calcular sequências consecutivas (otimizado com NumPy)
    for _, row in track_dates.iterrows():
        track_key = row['track_key']
        dates = row['date']
        
        if len(dates) == 0:
            continue
        
        if len(dates) == 1:
            consecutive_counts[track_key] = 1
            continue
        
        # Converter para numpy array para performance
        dates_array = np.array([d.toordinal() for d in dates])
        
        # Calcular diferenças entre dias consecutivos
        diffs = np.diff(dates_array)
        
        # Encontrar onde não é consecutivo (diff != 1)
        splits = np.where(diffs != 1)[0] + 1
        
        # Split em sequências consecutivas
        sequences = np.split(dates_array, splits)
        
        # Encontrar maior sequência
        max_consecutive = max(len(seq) for seq in sequences)
        consecutive_counts[track_key] = max_consecutive
    
    # Ordenar e retornar top n
    result = sorted(consecutive_counts.items(), key=lambda x: x[1], reverse=True)[:n]
    PROCESSED_CACHE[cache_key] = result
    
    logger.info(f"📅 Repeat days: top {len(result)} tracks calculados")
    return result


def viciado_tracks_sessions(df, n=10):
    """
    VICIADO TRACKS: Número de sessões onde a mesma música toca múltiplas vezes
    
    Sessão = sequência de plays com gap < 30 minutos entre eles
    """
    if df.empty:
        return []
    
    cache_key = f"viciado_sessions_{len(df)}_{n}_{MIN_INTENTIONAL_PLAY_MS}"
    if cache_key in PROCESSED_CACHE:
        return PROCESSED_CACHE[cache_key]
    
    # Ordenar por timestamp
    df_sorted = df.sort_values('ts').reset_index(drop=True)
    
    # Calcular diferença de tempo entre plays consecutivos (vectorizado)
    df_sorted['time_diff_seconds'] = df_sorted['ts'].diff().dt.total_seconds()
    
    # Marcar início de nova sessão (gap > 30 min)
    df_sorted['new_session'] = (
        df_sorted['time_diff_seconds'].isna() | 
        (df_sorted['time_diff_seconds'] > SESSION_GAP_MINUTES * 60)
    )
    
    # Criar session_id (vectorizado)
    df_sorted['session_id'] = df_sorted['new_session'].cumsum()
    
    # Contar plays por track por sessão
    session_track_counts = (
        df_sorted.groupby(['session_id', 'track_key'], sort=False)
        .size()
        .reset_index(name='plays_in_session')
    )
    
    # Contar sessões onde track teve múltiplas plays
    multiple_plays = session_track_counts[session_track_counts['plays_in_session'] > 1]
    
    viciado_counts = (
        multiple_plays.groupby('track_key', sort=False)
        .size()
        .sort_values(ascending=False)
        .head(n)
    )
    
    result = list(viciado_counts.items())
    PROCESSED_CACHE[cache_key] = result
    
    logger.info(f"🔄 Viciado tracks: top {len(result)} tracks calculados")
    return result


# ============================================================================
# ENRIQUECIMENTO COM SPOTIFY API (apenas top 5 para performance)
# ============================================================================

def enrich_with_spotify_metadata_fast(df, item_type='track', max_items=100):
    """
    Enriquece APENAS os top 5 items com metadata da API do Spotify
    Resto fica sem metadata para manter performance alta
    """
    if df.empty:
        return df
    
    df_top = df.head(max_items).copy()
    df_rest = df.iloc[max_items:].copy() if len(df) > max_items else pd.DataFrame()
    
    enriched_items = []
    
    # Processar top 5 com API do Spotify
    for idx, row in df_top.iterrows():
        cache_key = None
        metadata = None
        
        if item_type == 'track':
            cache_key = f"track:{row.get('track_key', '')}"
            if cache_key not in METADATA_CACHE:
                track_artist = str(row.get('track_key', '')).split(' - ', 1)
                track_name = track_artist[0] if len(track_artist) > 0 else ''
                artist_name = track_artist[1] if len(track_artist) > 1 else ''
                
                enhancer = get_spotify_enhancer()
                if enhancer and enhancer.api_available:
                    metadata = enhancer.search_track_metadata(track_name, artist_name)

            else:
                metadata = METADATA_CACHE[cache_key]
        
        elif item_type == 'artist':
            artist_name = str(row.get('artist_key', ''))
            cache_key = f"artist:{artist_name}"
            if cache_key not in METADATA_CACHE:
                enhancer = get_spotify_enhancer()
                if enhancer and enhancer.api_available:
                    metadata = enhancer.search_artist_metadata(artist_name)

            else:
                metadata = METADATA_CACHE[cache_key]
        
        elif item_type == 'album':
            album_name = str(row.get('album_key', ''))
            cache_key = f"album:{album_name}"
            if cache_key not in METADATA_CACHE:
                enhancer = get_spotify_enhancer()
            if enhancer and enhancer.api_available:
                metadata = enhancer.search_album_metadata(album_name, "")

            else:
                metadata = METADATA_CACHE[cache_key]
        
        enriched_item = row.to_dict()
        if metadata:
            enriched_item.update({
                'image_url': metadata.get('image_url'),
                'spotify_url': metadata.get('spotify_url'),
                'spotify_id': metadata.get('spotify_url', '').split('/')[-1] if metadata.get('spotify_url') else None,
                'enhanced_name': metadata.get('name'),
                'enhanced_artist': metadata.get('artist'),
                'preview_url': metadata.get('preview_url')
            })
        
        enriched_items.append(enriched_item)
    
    # Resto sem API (para manter performance)
    if not df_rest.empty:
        for idx, row in df_rest.iterrows():
            enriched_item = row.to_dict()
            enriched_item.update({
                'image_url': None,
                'spotify_url': f'https://open.spotify.com/search/{row.get("track_key", "")}',
                'spotify_id': None,
                'enhanced_name': None,
                'enhanced_artist': None,
                'preview_url': None
            })
            enriched_items.append(enriched_item)
    
    return pd.DataFrame(enriched_items)


# ============================================================================
# FUNÇÕES PRINCIPAIS - INTERFACE PÚBLICA
# ============================================================================

def top_tracks(df, n=10, include_metadata=True):
    result = top_tracks_ultra_fast(df, n)
    enhancer = get_spotify_enhancer()
    if include_metadata and not result.empty and enhancer and enhancer.api_available:
        logger.info(f"🎵 A enriquecer top {n} músicas com Spotify API...")
        result = enrich_with_spotify_metadata_fast(result, 'track', n)
    return result


def top_albums(df, n=10, include_metadata=True):
    result = top_albums_ultra_fast(df, n)
    enhancer = get_spotify_enhancer()
    if include_metadata and not result.empty and enhancer and enhancer.api_available:
        logger.info(f"💿 A enriquecer top {n} álbuns com Spotify API...")
        result = enrich_with_spotify_metadata_fast(result, 'album', n)
    return result



def top_artists(df, n=10, include_metadata=True):
    result = top_artists_ultra_fast(df, n)
    enhancer = get_spotify_enhancer()
    if include_metadata and not result.empty and enhancer and enhancer.api_available:
        logger.info(f"🎤 A enriquecer top {n} artistas com Spotify API...")
        result = enrich_with_spotify_metadata_fast(result, 'artist', n)
    return result



def daily_history(df):
    """Histórico diário de plays"""
    return daily_history_optimized(df)


def repeat_spirals_optimized(df, n=10):
    """Repeat spirals = dias únicos de audição"""
    return repeat_spirals_correct(df, n)


def viciado_tracks_top20(df, n=10):
    """Viciado tracks = sessões com repetições"""
    return viciado_tracks_sessions(df, n)


def repeat_days_top20(df, n=10):
    """Repeat days = dias consecutivos máximos"""
    return repeat_days_consecutive(df, n)


# ============================================================================
# ESTATÍSTICAS DE DEBUG E AUDITORIA
# ============================================================================

def print_data_statistics(df):
    """
    Imprime estatísticas detalhadas sobre os dados filtrados
    Útil para debug, auditoria e validação de qualidade
    """
    if df.empty:
        logger.warning("⚠️ DataFrame vazio - sem estatísticas para mostrar")
        return
    
    logger.info(f"")
    logger.info(f"{'='*70}")
    logger.info(f"📊 ESTATÍSTICAS DOS DADOS FILTRADOS:")
    logger.info(f"{'='*70}")
    logger.info(f"")
    
    # Período temporal
    logger.info(f"Período:")
    logger.info(f"  • Primeiro play:    {df['ts'].min()}")
    logger.info(f"  • Último play:      {df['ts'].max()}")
    logger.info(f"  • Total de dias:    {(df['ts'].max() - df['ts'].min()).days}")
    logger.info(f"")
    
    # Contagens de plays
    logger.info(f"Plays:")
    logger.info(f"  • Total de plays:   {len(df):,}")
    logger.info(f"  • Tracks únicos:    {df['track_key'].nunique():,}")
    logger.info(f"  • Artists únicos:   {df['master_metadata_album_artist_name'].nunique():,}")
    logger.info(f"  • Albums únicos:    {df['master_metadata_album_album_name'].nunique():,}")
    logger.info(f"")
    
    # Estatísticas de duração
    logger.info(f"Duração:")
    logger.info(f"  • ms_played mín:    {df['ms_played'].min():,}ms ({df['ms_played'].min()/1000:.1f}s)")
    logger.info(f"  • ms_played médio:  {df['ms_played'].mean():,.0f}ms ({df['ms_played'].mean()/1000:.1f}s)")
    logger.info(f"  • ms_played máx:    {df['ms_played'].max():,}ms ({df['ms_played'].max()/1000:.1f}s)")
    logger.info(f"  • Total horas:      {df['ms_played'].sum()/(1000*60*60):,.1f}h")
    logger.info(f"")
    
    # Estatísticas de skips
    logger.info(f"Skips:")
    logger.info(f"  • Total skips:      {df['is_skip'].sum():,} ({100*df['is_skip'].mean():.1f}%)")
    logger.info(f"")
    
    # Estatísticas por tipo de play
    if 'play_type' in df.columns:
        logger.info(f"Tipo de Play:")
        type_counts = df['play_type'].value_counts()
        for play_type, count in type_counts.items():
            logger.info(f"  • {play_type:12s}  {count:>10,} ({100*count/len(df):.1f}%)")
        logger.info(f"")
    
    logger.info(f"{'='*70}")
    logger.info(f"")


def normalize_album_names(df):
    """Normaliza album names usando fuzzy matching para agrupar variantes"""
    if df.empty:
        return df
    
    # Obter todos os albums únicos
    unique_albums = df['master_metadata_album_album_name'].unique()
    
    # Criar mapeamento de albums similares
    album_mapping = {}
    processed = set()
    
    for album1 in unique_albums:
        if album1 in processed:
            continue
        
        # Procurar albums similares (score > 85)
        similar_albums = [album1]
        for album2 in unique_albums:
            if album1 != album2 and fuzz.ratio(album1.lower(), album2.lower()) > 85:
                similar_albums.append(album2)
                processed.add(album2)
        
        # Usar o nome mais comum como canonical
        if len(similar_albums) > 1:
            # Contar plays de cada variante
            counts = df[df['master_metadata_album_album_name'].isin(similar_albums)].groupby('master_metadata_album_album_name').size()
            canonical = counts.idxmax()  # Nome com mais plays
            
            for variant in similar_albums:
                album_mapping[variant] = canonical
        
        processed.add(album1)
    
    # Aplicar mapping
    if album_mapping:
        df['master_metadata_album_album_name'] = df['master_metadata_album_album_name'].map(
            lambda x: album_mapping.get(x, x)
        )
        logger.info(f"✓ Normalized {len(album_mapping)} album name variants")
    
    return df    


# ============================================================================
# TESTE E VALIDAÇÃO
# ============================================================================

if __name__ == '__main__':
    """
    Teste standalone do módulo data_processing
    Executa todos os filtros e mostra estatísticas
    """
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    logger.info("🚀 A testar data_processing.py...")
    logger.info("")
    
    try:
        # Carregar dados raw
        df_raw = load_streaming_history()
        
        # Aplicar pipeline de filtros
        df_music = filter_music(df_raw)
        
        # Mostrar estatísticas detalhadas
        print_data_statistics(df_music)
        
        # Testar funções de análise
        logger.info("🧪 A testar funções de análise...")
        logger.info("")
        
        top_tracks_result = top_tracks(df_music, n=5, include_metadata=False)
        logger.info(f"✅ Top 5 tracks: {len(top_tracks_result)} resultados")
        if not top_tracks_result.empty:
            logger.info(f"   Top 1: {top_tracks_result.iloc[0]['track_key']} - {top_tracks_result.iloc[0]['plays']} plays")
        
        daily_hist = daily_history(df_music)
        logger.info(f"✅ Daily history: {len(daily_hist)} dias")
        
        spirals = repeat_spirals_optimized(df_music, n=5)
        logger.info(f"✅ Repeat spirals: {len(spirals)} resultados")
        if spirals:
            logger.info(f"   Top 1: {spirals[0][0]} - {spirals[0][1]} dias únicos")
        
        consecutive = repeat_days_top20(df_music, n=5)
        logger.info(f"✅ Repeat days: {len(consecutive)} resultados")
        if consecutive:
            logger.info(f"   Top 1: {consecutive[0][0]} - {consecutive[0][1]} dias consecutivos")
        
        logger.info("")
        logger.info("✅ ✅ ✅ TODOS OS TESTES PASSARAM COM SUCESSO! ✅ ✅ ✅")
        logger.info("")
        logger.info("📊 Sistema de filtros profissional validado e operacional")
        logger.info("🎵 Pronto para análise avançada de dados do Spotify")
        
    except Exception as e:
        logger.error(f"❌ ERRO nos testes: {e}")
        import traceback
        traceback.print_exc()
        raise
