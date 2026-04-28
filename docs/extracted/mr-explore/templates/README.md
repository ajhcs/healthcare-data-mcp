# Built-in Templates

This directory contains pre-configured import templates for common hospital systems.

## Available Templates

### 1. CMS Standard (`cms_standard.hstpl`)
**Use for:** Most CMS-compliant hospital price transparency files

The default template. Works with the standard CMS machine-readable file format that most hospitals use for price transparency compliance.

**Column Mappings:** 21 fields
**Preprocessing:** Skip 2 header rows

### 2. Epic MyChart (`epic_mychart.hstpl`)
**Use for:** Epic system exports (MyChart, Hyperspace, etc.)

Epic is one of the largest EHR vendors. This template handles their specific column naming conventions and export format.

**Column Mappings:** 17 fields
**Preprocessing:** Skip 1 header row, UTF-8 BOM handling

### 3. Cerner Standard (`cerner_standard.hstpl`)
**Use for:** Cerner/Oracle Health system exports

For hospitals using Cerner (now Oracle Health) systems. Includes mappings for their specific field names.

**Column Mappings:** 17 fields
**Preprocessing:** Skip 3 metadata rows

### 4. Meditech Expanse (`meditech_standard.hstpl`)
**Use for:** Meditech system exports

Common in smaller community hospitals. Handles Meditech's unique column naming.

**Column Mappings:** 16 fields
**Preprocessing:** Skip 1 header row

## Using These Templates

### From the Application
1. Open: **Data > Template Library** (Ctrl+T)
2. Select a built-in template
3. Click "Use Template"
4. Import your file: **File > Open Hospital File(s)**

### Customizing
Built-in templates cannot be edited directly, but you can:
1. Select a template
2. Click "Duplicate"
3. Edit the copy
4. Save to your user templates

## Template File Format

Templates are JSON files with `.hstpl` extension:

```json
{
  "name": "Template Name",
  "vendor": "Vendor Name",
  "version": "1.0",
  "author": "Author",
  "description": "What this template is for",
  "column_mappings": {
    "CSV_Column_Name": "internal_field_name"
  },
  "preprocessing": [
    {
      "type": "skip_rows",
      "count": 2
    }
  ],
  "created": "2024-01-01",
  "updated": "2024-01-01"
}
```

## Internal Field Names

When creating custom templates, map CSV columns to these internal field names:

### Core Fields
- `description` - Charge description
- `code_1` - Primary procedure code
- `code_1_type` - Primary code type (CPT, HCPCS, etc.)
- `code_2` - Secondary code
- `code_2_type` - Secondary code type
- `modifiers` - Procedure modifiers

### Charge Amounts
- `gross_charge` - Standard/gross charge
- `discounted_cash` - Cash/self-pay price
- `negotiated_dollar` - Negotiated rate (dollar)
- `negotiated_percentage` - Negotiated rate (%)
- `min_charge` - Minimum charge
- `max_charge` - Maximum charge

### Payer Information
- `payer_name` - Insurance payer name
- `plan_name` - Insurance plan name
- `methodology` - Pricing methodology

### Settings
- `setting` - Care setting (inpatient/outpatient)
- `billing_class` - Billing classification

### Additional
- `additional_notes` - Notes/comments
- `drug_unit_of_measurement` - Drug unit
- `drug_type_of_measurement` - Drug measurement type

## Preprocessing Rules

### Skip Rows
Skip header/metadata rows:
```json
{
  "type": "skip_rows",
  "count": 2,
  "description": "Skip CMS header rows"
}
```

### Encoding
Specify file encoding:
```json
{
  "type": "encoding",
  "value": "utf-8-sig",
  "description": "Handle UTF-8 BOM"
}
```

## Creating Your Own Templates

### From Existing Template
1. Duplicate a similar template
2. Modify name and metadata
3. Adjust column mappings
4. Save with new name

### From Scratch
1. Get a sample CSV from your hospital
2. Note the exact column names
3. Create new template in Template Library
4. Map each CSV column to internal field
5. Add preprocessing rules if needed
6. Test with sample file
7. Save to user templates

## Sharing Templates

### Export
1. Select template in Template Library
2. Click "Export"
3. Save `.hstpl` file
4. Share file with colleagues

### Import
1. Receive `.hstpl` file
2. Open Template Library
3. Click "Import..."
4. Select file
5. Template added to user library

## Best Practices

1. **Test first**: Use small sample files to test mappings
2. **Document well**: Add clear descriptions
3. **Version properly**: Increment version when changing
4. **Name clearly**: Use vendor and product name
5. **Share often**: Export templates for your team

## Troubleshooting

### Template Not Working?
- Verify CSV column names match exactly
- Check preprocessing rules (skip_rows count)
- Try with smaller test file first
- Check for special characters in column names

### Columns Not Mapping?
- Column names are case-sensitive
- Check for leading/trailing spaces
- Some CSVs use BOM encoding - add encoding rule

### Import Fails?
- Verify skip_rows count is correct
- Check file encoding (UTF-8, UTF-8-BOM, etc.)
- Look for malformed CSV data

## Getting Help

- Full documentation: `../docs/TEMPLATE_LIBRARY.md`
- Quick start: `../docs/TEMPLATE_QUICK_START.md`
- UI guide: `../docs/TEMPLATE_UI_GUIDE.md`
- Demo script: `../examples/template_library_demo.py`
- Run tests: `pytest ../tests/test_template_library.py`

## Contributing Templates

Have a template for a system not listed here?
1. Create the template
2. Test it thoroughly
3. Export to .hstpl file
4. Share with the community

Common systems still needed:
- Allscripts
- Athenahealth
- NextGen
- eClinicalWorks
- CPSI

## Version History

- **v1.0** (2024-01-24): Initial release
  - CMS Standard
  - Epic MyChart
  - Cerner Standard
  - Meditech Expanse
