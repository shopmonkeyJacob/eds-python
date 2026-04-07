"""EDS version — set at build time via EDS_VERSION env var."""

import os

CURRENT: str = os.environ.get("EDS_VERSION", "dev")
