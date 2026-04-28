# MRF Templates

This directory contains custom MRF (Machine-Readable File) templates for hospital price transparency data.

## Built-in Templates

The following templates are built into the application:

1. **CMS Standard** - Standard CMS hospital price transparency format
2. **Epic MyChart** - Epic MyChart hospital price export format
3. **Cerner Standard** - Cerner Millennium price transparency export
4. **Meditech** - Meditech Expanse price transparency format
5. **CMS JSON** - CMS JSON machine-readable format

## Custom Templates

You can create custom templates by:

1. Using the template editor in the application
2. Manually creating JSON files in this directory
3. Importing templates from other users

## Template Format

Each template is stored as a JSON file with the following structure:

```json
{
  "name": "Template Name",
  "vendor": "Vendor Name",
  "version": "1.0",
  "description": "Description of the template",
  "column_mappings": {
    "description": "Vendor Column Name",
    "code_1": "Vendor Code Column",
    ...
  },
  "preprocessing": [
    "skip_header_rows:2",
    "normalize_nulls"
  ],
  "file_format": "csv",
  "encoding": "utf-8",
  "header_rows": 2,
  "delimiter": ",",
  "date_format": "%Y-%m-%d"
}
```

## Available Preprocessing Steps

- `skip_header_rows:N` - Skip first N rows
- `remove_footer_rows:N` - Remove last N rows
- `trim_whitespace` - Remove leading/trailing whitespace
- `normalize_nulls` - Convert various null representations to NULL
- `flatten_json` - Flatten nested JSON structures
- `extract_metadata` - Extract hospital metadata from header
- `remove_empty_rows` - Remove rows with all null values
- `convert_encoding` - Convert file encoding

## Standard Field Names

When creating custom templates, map vendor columns to these standard fields:

- `description` - Service/procedure description
- `code_1` - Primary code (CPT, HCPCS, etc.)
- `code_1_type` - Primary code type
- `code_2` - Secondary code
- `code_2_type` - Secondary code type
- `modifiers` - Code modifiers
- `setting` - Care setting (inpatient/outpatient)
- `billing_class` - Billing classification
- `gross_charge` - Gross charge amount
- `discounted_cash` - Discounted cash price
- `payer_name` - Insurance payer name
- `plan_name` - Insurance plan name
- `negotiated_dollar` - Negotiated dollar amount
- `negotiated_percentage` - Negotiated percentage
- `negotiated_algorithm` - Negotiation methodology
- `estimated_amount` - Estimated amount
- `methodology` - Pricing methodology
- `min_charge` - Minimum charge
- `max_charge` - Maximum charge
- `additional_notes` - Additional notes/comments

## Example Custom Template

See `example_custom_template.json` for a complete example.
