# MRF Import Wizard

A 5-step PyQt6 wizard for importing CMS Machine-Readable Files (MRF) with template-based parsing and classic Windows XP styling.

## Overview

The MRF Import Wizard provides a user-friendly interface for importing hospital price transparency files. It automatically detects file formats, validates column mappings, and tracks import progress.

## File Location

- **Main Module:** `src/ui/mrf_import_wizard.py`
- **Tests:** `tests/test_mrf_import_wizard.py`
- **Demo:** `demo_mrf_wizard.py`

## Architecture

### Components

1. **MRFImportWizard** - Main wizard controller
2. **FileSelectionPage** - Step 1: File selection
3. **TemplateDetectionPage** - Step 2: Template detection/selection
4. **ColumnMappingPage** - Step 3: Column mapping preview
5. **ImportProgressPage** - Step 4: Import execution
6. **SummaryPage** - Step 5: Results summary

### Dependencies

- **MRFConnector** (`src/data/connectors/mrf_connector.py`) - Handles file parsing
- **TemplateAwareImporter** (`src/data/sources/mrf/importer.py`) - Template-based import logic
- **TemplateManager** (`src/data/sources/mrf/templates.py`) - Template detection and management

## Usage

### Basic Usage

```python
from PyQt6.QtWidgets import QApplication
from src.ui.mrf_import_wizard import MRFImportWizard

app = QApplication([])
wizard = MRFImportWizard()

# Connect to completion signal
def on_import_completed(result):
    if result.success:
        print(f"Imported {result.row_count} records")
    else:
        print(f"Import failed: {result.error_message}")

wizard.import_completed.connect(on_import_completed)

# Show wizard
wizard.exec()

# Get result after completion
result = wizard.get_import_result()
```

### Integration Example

```python
# In main_window.py
from .mrf_import_wizard import MRFImportWizard

def import_mrf_file(self):
    """Open MRF import wizard."""
    wizard = MRFImportWizard(self)
    wizard.import_completed.connect(self._on_mrf_imported)
    wizard.exec()

def _on_mrf_imported(self, result):
    """Handle successful MRF import."""
    if result.success:
        # Process imported data
        tables = result.tables
        charges_df = tables.get("charges")

        # Update UI
        self._refresh_data()

        # Show confirmation
        QMessageBox.information(
            self,
            "Import Complete",
            f"Successfully imported {result.row_count:,} records"
        )
```

## Wizard Steps

### Step 1: File Selection

**Purpose:** Select MRF file to import

**Features:**
- File browser dialog (CSV, JSON, TXT)
- File information display (name, type, size, location)
- Validation: file must exist

**Completion:** File selected and exists

### Step 2: Template Detection

**Purpose:** Detect or manually select import template

**Features:**
- **Auto-detect mode:**
  - Analyzes file structure
  - Detects vendor format (Epic, Cerner, Meditech, CMS)
  - Shows confidence score

- **Manual mode:**
  - Dropdown with all available templates
  - Template info display (vendor, version, description)

**Supported Templates:**
- CMS Standard
- CMS JSON
- Epic MyChart
- Cerner Standard
- Meditech
- Custom (user-defined)

**Completion:** Template selected (auto-detected or manual)

### Step 3: Column Mapping Preview

**Purpose:** Verify column mappings and view sample data

**Features:**
- **Mapping table:**
  - Shows standard field → file column mappings
  - Sortable, scrollable

- **Sample data preview:**
  - First 5 rows of actual data
  - First 10 columns visible
  - Read-only display

**Completion:** Auto-complete (always valid)

### Step 4: Import Progress

**Purpose:** Execute import with progress tracking

**Features:**
- Progress bar (0-100%)
- Status messages from importer
- Detail text with current operation
- Non-blocking QThread worker
- Navigation disabled during import

**Progress Callbacks:**
```
5%   - Reading file...
10%  - Detecting format...
20%  - Parsing CSV/JSON...
50%  - Mapping columns...
70%  - Processing data types...
80%  - Extracting metadata...
100% - Complete!
```

**Completion:** Import finishes (success or failure)

### Step 5: Summary

**Purpose:** Display import results and warnings

**Features:**
- **Success summary:**
  - Total record count
  - Tables created with row counts
  - Metadata (hospital name, filename)

- **Warnings section:**
  - Missing data notifications
  - Data quality issues
  - Validation warnings

- **Error display:**
  - Error message if import failed
  - Suggestions for resolution

**Completion:** Auto-complete (always valid)

## Styling

The wizard uses classic Windows XP/7 styling to match the application:

### Color Scheme

- **Background:** `#ECE9D8` (Windows XP tan)
- **Selection:** `#316AC5` (Windows blue)
- **Border:** `#ACA899` (Windows gray)
- **Hover:** `#FFF8E1` (Light yellow)
- **Progress:** `#5C8AC7` (Blue)

### Fonts

- **Primary:** Tahoma, 11px
- **Fallback:** Segoe UI, sans-serif

### Button Style

```python
background-color: #ECE9D8
border: 2px outset #FFFFFF
padding: 4px 12px
min-width: 70px
```

### Group Box Style

```python
border: 2px groove #ACA899
border-radius: 4px
margin-top: 8px
padding-top: 12px
```

## API Reference

### MRFImportWizard

**Signals:**
- `import_completed(result: ConnectorResult)` - Emitted when import succeeds

**Methods:**
- `get_import_result() -> Optional[ConnectorResult]` - Get import result after completion

**Properties:**
- Classic Windows wizard style
- 5 pages
- No back button on start/last page
- Minimum size: 700x500

### FileSelectionPage

**Methods:**
- `get_file_path() -> Optional[Path]` - Get selected file path
- `isComplete() -> bool` - Check if file is selected

### TemplateDetectionPage

**Methods:**
- `get_selected_template() -> Optional[MRFTemplate]` - Get selected template
- `isComplete() -> bool` - Check if template is selected

**Modes:**
- Auto-detect (default)
- Manual selection

### ColumnMappingPage

**Methods:**
- `initializePage()` - Load mappings and sample data when page shown

**Features:**
- Mapping table with standard field mappings
- Sample data preview (first 5 rows, first 10 columns)

### ImportProgressPage

**Methods:**
- `get_import_result()` - Get import result
- `isComplete() -> bool` - Check if import finished

**Features:**
- Non-blocking QThread worker
- Progress bar and status updates
- Disables navigation during import

### SummaryPage

**Methods:**
- `initializePage()` - Load results when page shown

**Features:**
- HTML-formatted summary
- Warnings section (auto-hidden if no warnings)
- Error display for failed imports

## Testing

### Run Tests

```bash
pytest tests/test_mrf_import_wizard.py -v
```

### Test Coverage

- Wizard creation and structure
- Page titles and subtitles
- Page completion logic
- Template detection
- Styling verification
- Signal connections

### Demo Script

```bash
python demo_mrf_wizard.py
```

## Error Handling

### File Not Found

```python
if not file_path.exists():
    return ImportResult(
        success=False,
        error_message="File not found: {file_path}"
    )
```

### Template Detection Failure

```python
if template is None:
    # Switch to manual mode or show error
    self.detection_status.setText(
        "Could not auto-detect template. "
        "Please switch to manual selection."
    )
```

### Import Failure

```python
if not result.success:
    self.status_label.setText("Import failed")
    self.detail_label.setText(f"Error: {result.error_message}")
```

## Extension Points

### Custom Templates

Add new templates to `TemplateManager`:

```python
from src.data.sources.mrf.templates import MRFTemplate

custom_template = MRFTemplate(
    name="My Custom Format",
    vendor="Custom",
    version="1.0",
    column_mappings={
        "description": "PROC_DESC",
        "code_1": "PROC_CODE",
        # ... more mappings
    },
    header_rows=1,
    delimiter="|",
)

template_manager.save_template(custom_template)
```

### Custom Progress Callbacks

```python
def custom_progress_callback(percent, message):
    print(f"[{percent}%] {message}")
    # Custom progress tracking logic

importer = TemplateAwareImporter(
    progress_callback=custom_progress_callback
)
```

### Custom Validation

Override `isComplete()` in wizard pages:

```python
class CustomMappingPage(ColumnMappingPage):
    def isComplete(self) -> bool:
        # Custom validation logic
        if not self._validate_mappings():
            return False
        return super().isComplete()
```

## Known Limitations

1. **Large Files:** Very large files (>1GB) may take significant time to import
2. **JSON Preview:** Sample data preview not available for JSON files
3. **Template Detection:** May fail on heavily customized vendor formats
4. **Cancel During Import:** Import cannot be cancelled once started
5. **Progress Accuracy:** Progress percentages are estimates, not exact

## Future Enhancements

- [ ] Add cancel button for long-running imports
- [ ] Improve template detection accuracy
- [ ] Add JSON sample data preview
- [ ] Support for Excel (.xlsx) files
- [ ] Template creation wizard
- [ ] Import history tracking
- [ ] Batch import multiple files
- [ ] Custom field mapping editor
- [ ] Import validation rules
- [ ] Data quality reporting

## Troubleshooting

### Wizard Won't Open

Check PyQt6 installation:
```bash
pip install PyQt6
```

### Template Detection Fails

- Try manual template selection
- Check file format matches expected structure
- Verify file encoding (should be UTF-8)

### Import Progress Stuck

- Check file size (very large files take time)
- Verify file is not corrupted
- Check available memory

### Styling Issues

- Ensure Tahoma font is available
- Check Qt platform (Windows recommended for best styling)

## Credits

- **Author:** Claude (Anthropic)
- **Framework:** PyQt6
- **Design:** Classic Windows XP/7 theme
- **Task:** MR-Explore Task #4 (MR-Explore-fbz)

## License

Same as MR-Explore project.
