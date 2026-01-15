"""
RBC Security - SSL certificate configuration for RBC infrastructure.

Wraps the optional rbc_security package which is only available in RBC's
deployment environment. When running locally, the import fails gracefully
and the application continues without certificate configuration.
"""

try:
    import rbc_security
    _RBC_SECURITY_AVAILABLE = True
except ImportError:
    _RBC_SECURITY_AVAILABLE = False


def configure_ssl():
    """Enable RBC SSL certificates if available.

    Returns:
        True if certificates were enabled, False if unavailable.
    """
    if not _RBC_SECURITY_AVAILABLE:
        print("  rbc_security not available, skipping SSL configuration")
        return False

    print("  Enabling RBC Security certificates...")
    rbc_security.enable_certs()
    print("  RBC Security certificates enabled")
    return True
