"""
Management command to set up daily and hourly metrics generation tasks.
"""
from django.core.management.base import BaseCommand
from django_celery_beat.models import PeriodicTask, CrontabSchedule
import json


class Command(BaseCommand):
    help = 'Set up daily and hourly metrics generation periodic tasks'

    def handle(self, *args, **options):
        # Create or get the crontab schedule for daily at 2 AM UTC
        daily_schedule, created = CrontabSchedule.objects.get_or_create(
            minute='0',
            hour='2',
            day_of_week='*',
            day_of_month='*',
            month_of_year='*',
        )
        
        if created:
            self.stdout.write(
                self.style.SUCCESS('Created crontab schedule for daily at 2 AM UTC')
            )
        else:
            self.stdout.write('Daily crontab schedule already exists')

        # Create or get the crontab schedule for hourly at minute 0
        hourly_schedule, created = CrontabSchedule.objects.get_or_create(
            minute='0',
            hour='*',
            day_of_week='*',
            day_of_month='*',
            month_of_year='*',
        )
        
        if created:
            self.stdout.write(
                self.style.SUCCESS('Created crontab schedule for hourly at minute 0')
            )
        else:
            self.stdout.write('Hourly crontab schedule already exists')

        # Create or update the daily periodic task
        daily_task, created = PeriodicTask.objects.update_or_create(
            name='generate-daily-metrics',
            defaults={
                'task': 'apps.core.tasks.metrics.generate_daily_metrics',
                'crontab': daily_schedule,
                'enabled': True,
                'kwargs': json.dumps({}),
            }
        )
        
        if created:
            self.stdout.write(
                self.style.SUCCESS('Created periodic task: generate-daily-metrics')
            )
        else:
            self.stdout.write('Updated periodic task: generate-daily-metrics')

        # Create or update the hourly periodic task
        hourly_task, created = PeriodicTask.objects.update_or_create(
            name='generate-hourly-metrics',
            defaults={
                'task': 'apps.core.task.metrics.generate_hourly_metrics',
                'crontab': hourly_schedule,
                'enabled': True,
                'kwargs': json.dumps({}),
            }
        )
        
        if created:
            self.stdout.write(
                self.style.SUCCESS('Created periodic task: generate-hourly-metrics')
            )
        else:
            self.stdout.write('Updated periodic task: generate-hourly-metrics')
        
        self.stdout.write(
            self.style.SUCCESS(
                f'Daily metrics task is scheduled to run at {daily_schedule.hour}:{daily_schedule.minute} UTC daily'
            )
        )
        self.stdout.write(
            self.style.SUCCESS(
                f'Hourly metrics task is scheduled to run every hour at minute {hourly_schedule.minute}'
            )
        )
