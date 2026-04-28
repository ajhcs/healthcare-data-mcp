# Data Pack Bundling Documentation

## Overview

This document describes how data packs are bundled into the MR-Explore Windows executable using PyInstaller.

## Data Pack Structure

Data packs are stored in `data/packs/<pack_name>/` and contain Parquet files with hospital price transparency data and entity information.

### Required Files

Each data pack should contain these Parquet files:

#### Core MRF Files
- `charges.parquet` - Hospital charge records (MRF data)
- `hospitals.parquet` - Hospital information
- `descriptions.parquet` - Procedure/service descriptions
- `payers.parquet` - Insurance payer information
- `plans.parquet` - Insurance plan details
- `algorithms.parquet` - Pricing algorithm definitions
- `methodologies.parquet` - Pricing methodology definitions

#### Entity Files (from IRS 990 data)
- `entities.parquet` - Health system/entity information (name, EIN, NPI, location)
- `entity_links.parquet` - Links between entities and hospitals/facilities

#### Metadata
- `metadata.json` - Pack metadata (name, version, creation date, counts)

## Philadelphia Hospitals Pack

The `philadelphia_hospitals` pack is bundled with the application as a sample/test dataset.

### Current Status

**Location**: `data/packs/philadelphia_hospitals/`

**Contains**: Stub/synthetic data for testing purposes

**Files**:
- algorithms.parquet (3 records)
- charges.parquet (1000 records)
- descriptions.parquet (50 records)
- entities.parquet (3 Philadelphia health systems)
- entity_links.parquet (3 links)
- hospitals.parquet (3 hospitals)
- methodologies.parquet (3 records)
- payers.parquet (5 payers)
- plans.parquet (10 plans)
- metadata.json

### Replacing with Real Data

To replace the stub data with actual Philadelphia hospital data:

1. Obtain CMS-compliant hospital price transparency files for Philadelphia hospitals
2. Place CSV files in `data_source/philadelphia/`
3. Build the pack:
   ```bash
   python scripts/build_parquet_pack.py data_source/philadelphia_hospitals.db philadelphia_hospitals
   ```
4. The script will create all required Parquet files in `data/packs/philadelphia_hospitals/`
5. If you have IRS 990 data for the health systems:
   ```bash
   python scripts/build_990_pack.py data_source/990_philadelphia/ --output data/packs/philadelphia_hospitals
   ```
   This will add/update the `entities.parquet` and `entity_links.parquet` files

## Build Configuration

### PyInstaller Spec File

**File**: `MR-Explore.spec`

The spec file includes data packs in the `datas` collection:

```python
datas = [
    ('src', 'src'),
    ('data/packs', 'data/packs'),  # Bundle data packs
]
```

### Build Script

**File**: `build_app.bat`

The build script:
1. Activates the virtual environment
2. Runs PyInstaller with the spec file
3. Verifies data packs were bundled correctly

```batch
pyinstaller --noconfirm --log-level=WARN MR-Explore.spec
```

## Distribution Structure

After building, the data packs are located in:

```
dist/MR-Explore/
├── MR-Explore.exe
└── _internal/
    └── data/
        └── packs/
            ├── facility_settings.yaml
            ├── philadelphia_hospitals/
            │   ├── charges.parquet
            │   ├── hospitals.parquet
            │   ├── descriptions.parquet
            │   ├── payers.parquet
            │   ├── plans.parquet
            │   ├── algorithms.parquet
            │   ├── methodologies.parquet
            │   ├── entities.parquet
            │   ├── entity_links.parquet
            │   └── metadata.json
            └── [other packs...]
```

## Application Detection

The application detects data packs at startup in `src/ui/main_window.py`:

```python
packs_path = data_path / "packs"
# Look for packs with charges.parquet
for location in [
    packs_path / "main" / "charges.parquet",
    packs_path / "philadelphia_hospitals" / "charges.parquet",
    packs_path / "charges.parquet",
]:
    if location.exists():
        pack_name = location.parent.name if location.parent != packs_path else "main"
        self.db = DuckDBDatabase(packs_path, pack_name=pack_name)
        break
```

## Parquet File Schemas

### charges.parquet
```python
{
    "id": Int64,
    "hospital_id": Int32,
    "description_id": Int32,
    "code1": Utf8,
    "code1_type": Utf8,
    # ... (see src/data/duckdb_store.py for full schema)
}
```

### entities.parquet
```python
{
    "id": Int32,
    "name": Utf8,
    "ein": Utf8,           # Employer Identification Number
    "npi": Utf8,           # National Provider Identifier
    "location": Utf8,      # City, State
    "aliases": List(Utf8), # Alternative names
}
```

### entity_links.parquet
```python
{
    "entity_id": Int32,
    "source_type": Utf8,  # "hospital", "facility", "system"
    "source_id": Int32,   # ID in source table
    "confidence": Float64, # Link confidence (0.0-1.0)
}
```

## Testing the Build

To verify the build includes data packs:

```bash
# Run the build
build_app.bat

# The script will automatically verify:
# 1. Data packs directory exists
# 2. Parquet files are present
```

Manual verification:
```bash
# Check data packs were bundled
ls dist/MR-Explore/_internal/data/packs/

# Check philadelphia_hospitals pack
ls dist/MR-Explore/_internal/data/packs/philadelphia_hospitals/

# Count Parquet files
find dist/MR-Explore/_internal/data/packs -name "*.parquet" | wc -l
```

## Creating New Data Packs

### From SQLite Database
```bash
python scripts/build_parquet_pack.py <sqlite_path> <pack_name>
```

### Sample Pack (for testing)
```bash
python scripts/build_parquet_pack.py --sample
```

### From 990 Data
```bash
python scripts/build_990_pack.py <source_dir> --output data/packs/<pack_name>
```

## Compression

All Parquet files use ZSTD compression (level 3) for optimal balance between:
- Compression ratio (typically 60-80% savings vs SQLite)
- Read performance (DuckDB can query compressed Parquet very efficiently)
- Write performance

## Notes

- Data packs are read-only at runtime
- DuckDB queries Parquet files directly without loading into memory
- The `entities.parquet` file integrates IRS 990 data for entity resolution
- Multiple packs can be bundled, but only one is active at startup
- Pack detection prioritizes: main > philadelphia_hospitals > flat structure

## Related Files

- `src/data/duckdb_adapter.py` - DuckDB adapter that loads Parquet packs
- `src/data/duckdb_store.py` - Parquet schemas and creation functions
- `src/data/entities.py` - Entity management using entities.parquet
- `src/data/connectors/irs990_connector.py` - IRS 990 data parser
- `scripts/build_parquet_pack.py` - Pack builder from SQLite
- `scripts/build_990_pack.py` - 990 data pack builder
