"""
Development settings for DRF project.
"""

import os
from .base import *

# Development-specific settings
DEBUG = True

# Add Django Debug Toolbar settings (if available)
if DEBUG:
    try:
        import debug_toolbar
        INSTALLED_APPS += ["debug_toolbar"]
        MIDDLEWARE = ["debug_toolbar.middleware.DebugToolbarMiddleware"] + MIDDLEWARE

        # Debug toolbar settings
        INTERNAL_IPS = [
            "127.0.0.1",
        ]
    except ImportError:
        pass

# Configure logging for development
LOGGING["handlers"]["console"]["level"] = "INFO"
LOGGING["loggers"]["django"]["level"] = "INFO"

# Add django.db.backends logger for database query logging
LOGGING["loggers"]["django.db.backends"] = {
    "handlers": ["console"],
    "level": "INFO",
    "propagate": False,
}
