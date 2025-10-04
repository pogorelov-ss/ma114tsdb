"""
ma114tsdb API Application

Polylith project combining API base with observability components.
"""

# Re-export API main for convenience
from bases.api.main import main, app

__all__ = ["main", "app"]
