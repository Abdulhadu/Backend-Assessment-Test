from celery import shared_task
from django.utils import timezone
from datetime import timedelta
import logging
import json
import os
from pathlib import Path

logger = logging.getLogger(__name__)



@shared_task
def send_notification_email(tenant_id, subject, message, recipient_email):
    """
    Send notification email to a specific recipient.
    """
    from django.core.mail import send_mail
    from django.conf import settings
    
    try:
        send_mail(
            subject=subject,
            message=message,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=[recipient_email],
            fail_silently=False,
        )
        logger.info(f"Notification email sent to {recipient_email}")
        return f"Email sent to {recipient_email}"
    except Exception as e:
        logger.error(f"Failed to send email to {recipient_email}: {str(e)}")
        raise