# Procfile for Render deployment
web: python main.py
worker: celery -A tasks.celery_app worker --loglevel=info
beat: celery -A tasks.celery_app beat --loglevel=info

