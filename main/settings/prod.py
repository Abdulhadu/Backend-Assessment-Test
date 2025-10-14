"""
Production settings for DRF project.
"""

import os
from .base import *

# Production specific settings
DEBUG = False

# Security settings
SECURE_SSL_REDIRECT = True
SECURE_HSTS_SECONDS = 31536000  # 1 year
SECURE_HSTS_INCLUDE_SUBDOMAINS = True
SECURE_HSTS_PRELOAD = True
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_BROWSER_XSS_FILTER = True
SECURE_REFERRER_POLICY = "strict-origin-when-cross-origin"
X_FRAME_OPTIONS = "DENY"

# Session security
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_AGE = 3600  # 1 hour

# CSRF security
CSRF_COOKIE_SECURE = True
CSRF_COOKIE_HTTPONLY = True

# Database connection pooling
DATABASES["default"]["CONN_MAX_AGE"] = 60

# Static files - Use WhiteNoise for serving static files
MIDDLEWARE.insert(1, "whitenoise.middleware.WhiteNoiseMiddleware")
STATICFILES_STORAGE = "whitenoise.storage.CompressedManifestStaticFilesStorage"

# CORS - Restrict origins in production
CORS_ALLOW_ALL_ORIGINS = False
cors_origins = os.environ.get("CORS_ALLOWED_ORIGINS", "")
CORS_ALLOWED_ORIGINS = [
    origin.strip() for origin in cors_origins.split(",") if origin.strip()
]

# Email settings for production
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"

# Logging - Log to files in production
LOGGING["handlers"]["file"]["filename"] = "/var/log/django/django.log"
LOGGING["handlers"]["error_file"] = {
    "level": "ERROR",
    "class": "logging.FileHandler",
    "filename": "/var/log/django/django_error.log",
    "formatter": "verbose",
}
LOGGING["loggers"]["django"]["handlers"] = ["file", "error_file"]

# Cache - Use database cache
CACHES["default"]["KEY_PREFIX"] = "drf_prod"

# AWS S3 settings (if using S3 for static/media files)
if os.environ.get("AWS_ACCESS_KEY_ID"):
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_STORAGE_BUCKET_NAME = os.environ.get("AWS_STORAGE_BUCKET_NAME")
    AWS_S3_REGION_NAME = os.environ.get("AWS_S3_REGION_NAME", "us-east-1")
    AWS_S3_CUSTOM_DOMAIN = f"{AWS_STORAGE_BUCKET_NAME}.s3.amazonaws.com"
    AWS_DEFAULT_ACL = "public-read"
    AWS_S3_OBJECT_PARAMETERS = {
        "CacheControl": "max-age=86400",
    }

    # Static files
    STATICFILES_STORAGE = "storages.backends.s3boto3.S3Boto3Storage"
    STATIC_URL = f"https://{AWS_S3_CUSTOM_DOMAIN}/static/"

    # Media files
    DEFAULT_FILE_STORAGE = "storages.backends.s3boto3.S3Boto3Storage"
    MEDIA_URL = f"https://{AWS_S3_CUSTOM_DOMAIN}/media/"
