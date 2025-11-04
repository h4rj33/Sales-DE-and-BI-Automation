# 👨‍💻 Harpartap Sales DE and BI Automation Project — Azure Data Factory Part Readme

## 1. Objective

This project delivers a **modular, metadata-driven ETL pipeline** using **Azure Data Factory (ADF)** and **Azure PostgreSQL**. The solution is built to dynamically:

- Ingest multiple CSV files from Azure Blob Storage,
- Automatically create destination tables in PostgreSQL with inferred schemas,
- Append metadata such as insertion timestamps,
- Log both successful job runs and detailed error traces for observability and governance.

---

## 2. Folder Structure

```
/
├── pipelines/
│   └── pl_ingest_generic.json             (ADF pipeline definition)
├── datasets/
│   ├── ds_generic_csv.json                (Dataset for metadata fetch)
│   ├── ds_csv_input.json                  (Dataset with file) parameterization
│   ├── ds_generic_csv_copy1.json          (Source dataset used in data) ingestion
│   └── ds_pg_dynamic.json                 (PostgreSQL sink dataset) (parameterized)
├── linkedServices/
│   ├── AzureBlobStorageLS.json
│   └── AzurePostgreSQLLS.json
└── README.md                              (Project documentation)                          # Project documentation
```

---

## 3. Pipeline Overview: `pl_ingest_generic`

The pipeline comprises **modular ETL stages**. Here’s a breakdown of each layer:

### 3.1 Metadata Layer
- **`Get_csv_metadata`**: Uses ADF's `GetMetadata` to list all CSV files in a folder within Azure Blob Storage.

### 3.2 File Loop (ForEach)
For each file identified:
- **`Lookup - file firstrow`**: Reads the first row of the CSV to extract header (field names).
- **`Set precopy_script`**: Dynamically generates a PostgreSQL DDL statement using ADF expressions to:
  - Drop existing table if exists,
  - Create new table with columns from the file,
  - Add `etl_insert_date TIMESTAMP`.

### 3.3 Ingestion Layer
- **`data_ingested`** (Copy Activity):
  - Source: Parameterized CSV with `DelimitedTextSource`.
  - Sink: PostgreSQL table with name format `harpartap_<filename>`.
  - Adds a metadata column `etl_insert_date` for each row.

### 3.4 Logging Layer
- **`record_count`**: Stores the number of rows successfully ingested.
- **`status`**: Tracks success/failure of ingestion per file.
- **`harpartap_etl_job_logs`**: Inserts ETL run summary into a PostgreSQL logging table with pipeline name, timestamps, and record counts.

### 3.5 Error Handling
- **`error_message`**: Captures error message on failure.
- **`harpartap_etl_error_logs`**: Inserts error logs into PostgreSQL table with file name, error message, and timestamp.

---

## 4. Configurable Parameters and Variables

| Variable / Parameter     | Purpose                                                                 |
|--------------------------|-------------------------------------------------------------------------|
| `fileName`               | Dynamically binds current filename in ForEach loop                      |
| `tableName`              | Formatted as `harpartap_<filename>`                                     |
| `precopy_script`         | DDL for table recreation                                                 |
| `record_count`           | Captures number of rows inserted during copy                            |
| `status`                 | Logs pipeline success/failure                                            |
| `error_message`          | Captures exception details if ingestion fails                            |

---

## 5. Assumptions Made

- Input files are delimited text (`|` pipe-delimited) with a single header row.
- First row of the file contains valid field names without null/empty entries.
- Table names are derived from file names with prefix `harpartap_`.
- All columns are cast as `VARCHAR(1000)` for simplicity.
- `etl_insert_date` is added to all ingested records.
- All files are ingested in **sequential order** using `ForEach`.
- Tables are **recreated** on every run, replacing previous data.

---

## 6. Known Limitations

- **No incremental loading**: Existing data is overwritten.
- **No type inference**: All columns default to `VARCHAR(1000)` regardless of actual type.
- **Header-only inference**: Pipeline assumes file structure is consistent and clean.
- **Schema evolution unsupported**: If structure changes across files, ingestion may fail.
- **No nested folders**: Metadata activity does not recursively check subfolders.

---

## 7. Suggestions for Future Enhancements

1. **Schema Inference**: Use Azure Data Flow to automatically detect column types.
2. **Incremental Loads**: Add watermarking and last modified tracking.
3. **Advanced Error Handling**: Integrate alerts (e.g., email, Teams) on failure.
4. **Schema Registry Integration**: Store and validate schemas prior to ingestion.
5. **Parallel Processing**: Optimize `ForEach` to process files concurrently if required.

---

## 8. Logging Tables

Two PostgreSQL tables are used for observability:

### `harpartap_etl_job_logs`

| Column        | Description                             |
|---------------|-----------------------------------------|
| job_name      | Name of the pipeline                    |
| status        | Job status (`Success` / `Failed`)       |
| record_count  | Rows inserted                           |
| start_time    | Trigger start time                      |
| end_time      | Completion time                         |

### `harpartap_etl_error_logs`

| Column        | Description                             |
|---------------|-----------------------------------------|
| table_name    | Table where ingestion failed            |
| file_name     | CSV file that caused the failure        |
| error_message | Captured error message                  |
| timestamp     | Time of failure                         |

-------------------------------------------------------------------------

## 9. Conclusion

This pipeline provides a **scalable** and **automated ingestion framework** using Azure Data Factory and PostgreSQL, with essential components like dynamic schema creation, logging, and modular orchestration. Designed with extensibility in mind, the project can be enhanced further for complex enterprise-grade ingestion scenarios.

-------------------------------------------------------------------------

