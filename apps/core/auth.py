import hashlib
from typing import Optional
from django.http import JsonResponse
from rest_framework import status
from apps.tenants.models import Tenant  # adjust if Tenant is in another app


def authenticate_tenant(request, tenant_id: str) -> Optional[Tenant]:
    """
    Authenticate and validate a tenant based on the X-API-Key header and tenant_id.
    - Verifies if the API key exists (either raw or pre-hashed)
    - Ensures the tenant_id matches the tenant's actual ID
    Returns:
        Tenant object if authentication succeeds, else returns a JsonResponse (401 or 400)
    """
    api_key = request.headers.get('X-API-Key')
    if not api_key:
        return JsonResponse(
            {'error': 'X-API-Key header required'},
            status=status.HTTP_401_UNAUTHORIZED
        )

    # Build candidate hashes (raw + hashed)
    candidate_hashes = {api_key}
    try:
        candidate_hashes.add(hashlib.sha256(api_key.encode()).hexdigest())
    except Exception:
        pass

    try:
        tenant = Tenant.objects.get(api_key_hash__in=list(candidate_hashes))
    except Tenant.DoesNotExist:
        return JsonResponse(
            {'error': 'Invalid API key'},
            status=status.HTTP_401_UNAUTHORIZED
        )

    # Check tenant ID match
    if str(tenant.tenant_id) != str(tenant_id):
        return JsonResponse(
            {'error': 'Tenant ID mismatch'},
            status=status.HTTP_400_BAD_REQUEST
        )

    return tenant
