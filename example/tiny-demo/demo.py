# /// script
# dependencies = [
#   "asyncpg",
#   "pytest",
#   "pytest-asyncio",
#   "waymark",
# ]
# ///

import os

from waymark.workflow import workflow_registry

os.environ["WAYMARK_DATABASE_URL"] = "1"
workflow_registry._workflows.clear()
