# Full Product MS List (Telegram + PDF)

This document summarizes the implemented flow for:

- `Manage Production` -> `View MS Cut List` -> `View Full Product MS List`

## 1. User Flow

1. User opens `View Full Product MS List`.
2. Bot fetches product model codes from costing summary table:
   - `ProductModelConfig_summary_ProductModelCode`
   - field: `ProductModelCode_ProductModelCode`
3. Bot shows paginated numbered selection of model codes.
4. User picks a model code.
5. Bot fetches matching MS rows, builds PDF, and sends it as Telegram document attachment.

## 2. Data Source + Join Logic

Implemented in `pulse/data/costing_repo.py`:

- `get_product_model_codes()`
  - Reads from `ProductModelConfig_summary_ProductModelCode`
  - Returns unique `ProductModelCode_ProductModelCode`

- `get_full_ms_table_rows_for_product_model(model_code)`
  - Finds matching parts in `ProductModelConfig` where
    `ProductModelCode_ProductModelCode2 == model_code`
  - Collects `ProductPartName` IDs
  - Fetches `ProductPartMSList` rows for those part IDs
  - Resolves `MaterialToCut` reference via `MasterMaterial` table:
    - `ProductPartMSList.MaterialToCut` -> `MasterMaterial.id`
    - display value from `MasterMaterial.MasterMaterial`

## 3. Output Columns (Current)

The exported table includes:

1. `No.`
2. `Part Name`
3. `MaterialToCut` (resolved text from `MasterMaterial`)
4. `Length (mm)`
5. `Qty`
6. `Remarks`
7. `OptionGroup1_TEMP`

## 4. Filtering Rule (Current)

- Exclude rows where `Qty` is blank or `0`.

## 5. PDF Rendering

Implemented in:

- `pulse/utils/pdf_export.py` (`write_table_pdf`)
- `pulse/main.py` (`_send_full_product_ms_pdf`)

Behavior:

- Generates bordered table PDF (ReportLab Table).
- Repeats header on each page.
- Sends file via Telegram `reply_document`.

## 6. Row Color Grouping

- Entire row background is color-shaded by unique `Part Name`.
- All rows with the same `Part Name` share the same shade.
- Different `Part Name` groups use next color from configured palette (cyclic).

## 7. Config (No Code Change Needed)

Optional `.env` keys:

- `MSCUTLIST_PDF_COLUMN_WIDTHS`
  - JSON map in millimeters by header name.
  - Example:
  - `{"No.":12,"Part Name":55,"MaterialToCut":42,"Length (mm)":20,"Qty":14,"Remarks":28,"OptionGroup1_TEMP":28}`

- `MSCUTLIST_PDF_ROW_PALETTE`
  - Comma-separated hex colors.
  - Example:
  - `#f2f8ff,#eefaf2,#fff8ee,#f7f1ff,#edf7f7,#fff0f3,#f4f4ec`

If these are not set or invalid:

- Column widths fall back to ReportLab auto width.
- Row colors fall back to internal default pastel palette.

## 8. Main Integration Points

- Menu/selection state:
  - `pulse/menu/submenu.py`
  - `PRODUCT_MODEL_SELECTION_STATE`
  - product model paging + selection handlers

- Action routing:
  - `pulse/main.py`
  - target action `FULL_PRODUCT_MS_LIST`

- Dependency:
  - `requirements.txt` includes `reportlab`
