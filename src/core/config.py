import logging

DATABASE_URL = "sqlite+aiosqlite:///tasks.db"
NUM_WORKERS = 2

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.WARNING)
logging.getLogger("watchfiles.main").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("task_queue.log"), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)
