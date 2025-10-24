# Procfile para deploy em Render/Railway
# Define o comando para iniciar a aplicação em produção

web: gunicorn app:app --bind 0.0.0.0:$PORT --timeout 120 --workers 2 --threads 4 --worker-class gthread
