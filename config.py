# ============================================================
# Taxi Medallion Pipeline - Centralized Configuration
# ============================================================
# All data storage paths are defined here in one place.
# Update DATA_ROOT to change where all pipeline data is stored.

# -------------------------------------------------------------
# Storage Root Configuration
# -------------------------------------------------------------
# DATA_ROOT: Where all pipeline data is stored
# This should point to a Unity Catalog Volume (recommended)
# Format: /Volumes/<catalog>/<schema>/<volume>
DATA_ROOT = "/Volumes/workspace/default/taxi_data"

# Alternative: Use DBFS path (not recommended - DBFS may be disabled)
# DATA_ROOT = "/dbfs/taxi_data"

# Alternative: Use workspace path (not recommended - limited data features)
# DATA_ROOT = "/Workspace/Users/jaswanth.vudduru@gmail.com/taxi-medallion-pipeline/data"

# -------------------------------------------------------------
# Medallion Architecture Paths
# -------------------------------------------------------------
BRONZE_PATH = f"{DATA_ROOT}/bronze"
SILVER_PATH = f"{DATA_ROOT}/silver"
GOLD_PATH = f"{DATA_ROOT}/gold"

# -------------------------------------------------------------
# Raw Data Ingestion
# -------------------------------------------------------------
RAW_DATA_PATH = f"{DATA_ROOT}/raw"
DEAD_LETTER_PATH = f"{DATA_ROOT}/dead_letter"

# -------------------------------------------------------------
# Streaming Paths
# -------------------------------------------------------------
STREAM_SOURCE = f"{DATA_ROOT}/stream_input/"
STREAM_BRONZE = f"{DATA_ROOT}/stream_bronze"
CHECKPOINT_PATH = f"{DATA_ROOT}/checkpoints/bronze"

# -------------------------------------------------------------
# Demo & Testing Paths
# -------------------------------------------------------------
LIFECYCLE_DEMO_PATH = f"{DATA_ROOT}/lifecycle_demo"
TEMP_PATH = f"{DATA_ROOT}/temp"

# -------------------------------------------------------------
# Helper Function
# -------------------------------------------------------------
def get_path(path_name: str) -> str:
    """
    Get a configured path by name.
    
    Args:
        path_name: Name of the path (e.g., 'bronze', 'silver', 'gold')
    
    Returns:
        The configured path
    """
    paths = {
        'bronze': BRONZE_PATH,
        'silver': SILVER_PATH,
        'gold': GOLD_PATH,
        'raw': RAW_DATA_PATH,
        'dead_letter': DEAD_LETTER_PATH,
        'stream_source': STREAM_SOURCE,
        'stream_bronze': STREAM_BRONZE,
        'checkpoint': CHECKPOINT_PATH,
        'lifecycle_demo': LIFECYCLE_DEMO_PATH,
        'temp': TEMP_PATH,
    }
    return paths.get(path_name, f"{DATA_ROOT}/{path_name}")

# -------------------------------------------------------------
# Setup Instructions
# -------------------------------------------------------------
"""
To use this configuration:

1. In notebooks:
   import sys
   sys.path.append("/Workspace/Users/jaswanth.vudduru@gmail.com/taxi-medallion-pipeline")
   from config import BRONZE_PATH, SILVER_PATH, GOLD_PATH

2. In Python modules within the pipeline:
   from config import BRONZE_PATH, SILVER_PATH, GOLD_PATH

3. To change storage location:
   - Update DATA_ROOT at the top of this file
   - All other paths will automatically update

4. To create the Unity Catalog Volume:
   spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.taxi_data")
"""
