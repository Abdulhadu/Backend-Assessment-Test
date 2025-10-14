"""
Celery configuration for the Django project.
"""
import os
from celery import Celery
from django.conf import settings

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'main.settings.dev')

app = Celery('main')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

# Note: Beat schedule is managed via django_celery_beat database scheduler
# Remove hardcoded schedule to avoid conflicts with database scheduler
# app.conf.beat_schedule = {
#     'cleanup-expired-data': {
#         'task': 'apps.core.tasks.cleanup_expired_data',s
#         'schedule': 86400.0,  # Run daily
#     },
#     'update-stock-levels': {
#         'task': 'apps.stocks.tasks.update_stock_levels',
#         'schedule': 3600.0,  # Run hourly
#     },
#     'generate-daily-metrics': {
#         'task': 'apps.core.tasks.generate_daily_metrics',
#         'schedule': 86400.0,  # Run daily
#     },
# }

app.conf.timezone = 'UTC'

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
