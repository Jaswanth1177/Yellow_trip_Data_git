# Taxi Medallion Pipeline

Production-grade medallion architecture pipeline for taxi data processing on Databricks.

## Architecture

```
Bronze → Silver → Gold
  ↓        ↓        ↓
Raw   Cleaned  Aggregated
```

## Project Structure

```
taxi-medallion-pipeline/
├── config.py               # Centralized configuration (DATA PATHS)
├── utils/
│   ├── __init__.py
│   ├── schemas.py         # Schema definitions
│   ├── bronze_ingestion.py
│   └── ...
└── notebooks/
    ├── 02_schema_and_dead_letter.py
    ├── 03_delta_lifecycle.py
    ├── 04_streaming_incremental.py
    └── ...
```

## Configuration System

**All data storage paths are configured in `config.py`.** This keeps your code in Git while data stays in Unity Catalog Volumes.

### Quick Start

1. **Create the Unity Catalog Volume:**
   ```sql
   CREATE VOLUME IF NOT EXISTS workspace.default.taxi_data;
   ```

2. **Import the config in notebooks:**
   ```python
   import sys
   sys.path.append("/Workspace/Users/jaswanth.vudduru@gmail.com/taxi-medallion-pipeline")
   from config import BRONZE_PATH, SILVER_PATH, GOLD_PATH
   ```

3. **Use the paths:**
   ```python
   df.write.format("delta").save(BRONZE_PATH)
   ```

### Changing Storage Location

**To change where ALL data is stored, edit ONE line in `config.py`:**

```python
# Current configuration (Unity Catalog Volume - recommended)
DATA_ROOT = "/Volumes/workspace/default/taxi_data"

# Alternative: Different catalog/schema/volume
# DATA_ROOT = "/Volumes/prod/analytics/taxi"

# Alternative: DBFS (not recommended if DBFS is disabled)
# DATA_ROOT = "/dbfs/taxi_data"
```

All other paths automatically update when you change `DATA_ROOT`.

### Available Paths

| Config Variable | Default Path | Purpose |
|----------------|--------------|---------|
| `DATA_ROOT` | `/Volumes/workspace/default/taxi_data` | Root for all data |
| `BRONZE_PATH` | `{DATA_ROOT}/bronze` | Raw ingested data |
| `SILVER_PATH` | `{DATA_ROOT}/silver` | Cleaned data |
| `GOLD_PATH` | `{DATA_ROOT}/gold` | Aggregated metrics |
| `RAW_DATA_PATH` | `{DATA_ROOT}/raw` | Raw input files |
| `DEAD_LETTER_PATH` | `{DATA_ROOT}/dead_letter` | Corrupt/invalid rows |
| `STREAM_SOURCE` | `{DATA_ROOT}/stream_input/` | Streaming input |
| `STREAM_BRONZE` | `{DATA_ROOT}/stream_bronze` | Streaming bronze |
| `CHECKPOINT_PATH` | `{DATA_ROOT}/checkpoints/bronze` | Streaming checkpoints |
| `LIFECYCLE_DEMO_PATH` | `{DATA_ROOT}/lifecycle_demo` | Delta lifecycle demo |

## Why Not Store Data in `/Workspace/` Paths?

**`/Workspace/` is for code, not data:**

✅ **What belongs in `/Workspace/` (Git-tracked):**
- Notebooks (`.ipynb`)
- Python modules (`.py`)
- Configuration files
- Documentation

❌ **What does NOT belong in `/Workspace/`:**
- Data files
- Delta tables
- Parquet files
- Checkpoints

**Technical reasons:**
1. **Limited data operations** - Workspace paths don't support Delta Lake features (time travel, ACID transactions, OPTIMIZE)
2. **Not designed for Spark** - Workspace filesystem isn't optimized for distributed data processing
3. **Serverless restrictions** - Limited write support on serverless compute
4. **Git pollution** - Data files are large and change frequently; they don't belong in version control

**Use Unity Catalog Volumes for data:**
- ✅ Full Delta Lake support
- ✅ Governed by Unity Catalog
- ✅ Optimized for Spark workloads
- ✅ Works perfectly on serverless compute

## Environment Setup

### Unity Catalog Volume

The pipeline uses a Unity Catalog Volume for data storage. Create it with:

```sql
CREATE VOLUME IF NOT EXISTS workspace.default.taxi_data;
```

Verify it exists:
```sql
SHOW VOLUMES IN workspace.default;
```

### Python Path

All notebooks need to add the project root to their Python path:

```python
import sys
sys.path.append("/Workspace/Users/jaswanth.vudduru@gmail.com/taxi-medallion-pipeline")
```

This allows importing from `config` and `utils` modules.

## Notebooks

### 02_schema_and_dead_letter.py
- Schema enforcement with PERMISSIVE mode
- Dead letter pattern for corrupt rows
- Bronze ingestion with `_corrupt_record` handling

### 03_delta_lifecycle.py
- OPTIMIZE for small files problem
- Time travel queries
- VACUUM for cleanup

### 04_streaming_incremental.py
- Structured streaming with checkpoints
- Incremental processing
- Fault tolerance

## Development Workflow

1. **Write code** in notebooks/Python modules (stored in `/Workspace/`)
2. **Data flows through** Unity Catalog Volumes (configured in `config.py`)
3. **Commit code to Git** (code only, no data files)
4. **Data stays in Unity Catalog** (governed, production-ready storage)

## Git Integration

**What to commit:**
- `config.py` (with sensible defaults)
- `utils/*.py` (Python modules)
- `notebooks/*.py` (notebook exports)
- `README.md` (this file)

**What NOT to commit:**
- Data files
- `.databricks/` (local Databricks config)
- `__pycache__/` (Python cache)

**`.gitignore` recommended:**
```
__pycache__/
*.pyc
.databricks/
.DS_Store
```

## Troubleshooting

### ModuleNotFoundError: No module named 'config'

**Solution:** Restart the Python kernel or restart the notebook session:
```python
dbutils.library.restartPython()
```

Then re-run the cells that import from config.

### DBFS_DISABLED errors

**Cause:** Trying to write to `/tmp/`, `dbfs:/`, or other DBFS paths when DBFS is disabled.

**Solution:** All paths are now configured to use Unity Catalog Volumes. Make sure you're using the latest notebooks with `config` imports.

### Permission errors on Unity Catalog Volume

**Check permissions:**
```python
from config import DATA_ROOT
dbutils.fs.ls(DATA_ROOT)
```

**Create volume if missing:**
```sql
CREATE VOLUME IF NOT EXISTS workspace.default.taxi_data;
```

## Next Steps

1. Review `config.py` and adjust `DATA_ROOT` if needed
2. Run `CREATE VOLUME` SQL command
3. Test imports: `from config import BRONZE_PATH`
4. Run notebooks in order: 02 → 03 → 04
5. Commit code changes to Git

## Contact

For questions about this pipeline architecture, consult the Databricks documentation on:
- Unity Catalog Volumes
- Delta Lake
- Structured Streaming
- Medallion Architecture
