# Procfile for Render deployment
web: uvicorn main:app --host 0.0.0.0 --port $PORT
worker: celery -A tasks.celery_app worker --loglevel=info
beat: celery -A tasks.celery_app beat --loglevel=info

