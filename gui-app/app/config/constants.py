from pathlib import Path


APP_NAME = "minidbms GUI"
DEFAULT_POLICY = "lru"
DEFAULT_FRAMES = 64
SUPPORTED_POLICIES = ("lru", "clock", "nocache", "opt")
DEFAULT_SERVER_HOST = "localhost"
DEFAULT_SERVER_PORT = 5433
DEFAULT_RESULTS_DIRNAME = "results"

REPO_ROOT = Path(__file__).resolve().parents[3]
APP_ROOT = Path(__file__).resolve().parents[2]
SERVER_DIR = REPO_ROOT / "server"
SERVER_BIN = SERVER_DIR / "minidbms-server"
DATA_DIR = REPO_ROOT / "data"
RESULTS_DIR = APP_ROOT / DEFAULT_RESULTS_DIRNAME
