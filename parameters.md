# MCP Server Tools – User Input Parameters

This document outlines all user input parameters required by the MCP server tools, categorized by functionality and field types.

---

## 📁 File Profiler MCP Server Tools

### 🔹 profile_file
| Field Key  | Type | Field Type |
|------------|------|------------|
| file_path  | str  | Text Input |

---

### 🔹 profile_directory
| Field Key | Type | Field Type |
|-----------|------|------------|
| dir_path  | str  | Text Input |
| parallel  | bool | Boolean (checkbox) |

---

### 🔹 detect_relationships
| Field Key              | Type  | Field Type |
|-----------------------|-------|------------|
| dir_path              | str   | Text Input |
| confidence_threshold  | float | Number Input |

---

### 🔹 enrich_relationships
| Field Key   | Type        | Field Type |
|-------------|------------|------------|
| dir_path    | str        | Text Input |
| provider    | str        | Dropdown (`google`, `groq`, `openai`, `anthropic`) |
| model       | str / None | Text Input (optional) |
| incremental | bool       | Boolean (checkbox) |

---

### 🔹 check_enrichment_status
| Field Key | Type | Field Type |
|-----------|------|------------|
| dir_path  | str  | Text Input |

---

### 🔹 visualize_profile
| Field Key   | Type        | Field Type |
|-------------|------------|------------|
| chart_type  | str        | Dropdown (multiple chart options) |
| table_name  | str / None | Text Input (optional) |
| column_name | str / None | Text Input (optional) |
| theme       | str        | Dropdown (`dark`, `light`) |

---

### 🔹 list_supported_files
| Field Key | Type | Field Type |
|-----------|------|------------|
| dir_path  | str  | Text Input |

---

### 🔹 upload_file
| Field Key            | Type | Field Type |
|---------------------|------|------------|
| file_name           | str  | Text Input |
| file_content_base64 | str  | Text Input (multiline/large) |

---

### 🔹 get_quality_summary
| Field Key | Type | Field Type |
|-----------|------|------------|
| file_path | str  | Text Input |

---

### 🔹 query_knowledge_base
| Field Key | Type | Field Type |
|-----------|------|------------|
| question  | str  | Text Input |
| top_k     | int  | Number Input |

---

### 🔹 get_table_relationships
| Field Key  | Type | Field Type |
|------------|------|------------|
| table_name | str  | Text Input |

---

### 🔹 compare_profiles
| Field Key | Type | Field Type |
|-----------|------|------------|
| dir_path  | str  | Text Input |

---

## 🔗 Connector MCP Server Tools

### 🔹 connect_source
| Field Key      | Type  | Field Type |
|----------------|-------|------------|
| connection_id  | str   | Text Input |
| scheme         | str   | Dropdown (`s3`, `abfss`, `gs`, `snowflake`, `postgresql`) |
| credentials    | dict  | Sensitive Field |
| display_name   | str   | Text Input (optional) |
| test           | bool  | Boolean (checkbox) |

#### 🔐 Credentials (Scheme-Specific)

- **S3**
  - aws_access_key_id (Sensitive)
  - aws_secret_access_key (Sensitive)
  - region (Text) OR profile_name (Text)

- **ADLS**
  - connection_string (Sensitive) OR
  - tenant_id (Text), client_id (Text), client_secret (Sensitive)

- **GCS**
  - service_account_json (Sensitive)

- **Snowflake**
  - account (Text), user (Text), password (Sensitive), warehouse (Text), role (Text)

- **PostgreSQL**
  - connection_string (Sensitive) OR
  - host (Text), port (Number), user (Text), password (Sensitive), dbname (Text)

---

### 🔹 list_connections
_No user input required_

---

### 🔹 test_connection
| Field Key     | Type | Field Type |
|---------------|------|------------|
| connection_id | str  | Text Input |

---

### 🔹 remove_connection
| Field Key     | Type | Field Type |
|---------------|------|------------|
| connection_id | str  | Text Input |

---

### 🔹 list_tables
| Field Key     | Type        | Field Type |
|---------------|------------|------------|
| uri           | str        | Text Input |
| connection_id | str / None | Text Input (optional) |

---

### 🔹 list_schemas
| Field Key     | Type        | Field Type |
|---------------|------------|------------|
| uri           | str        | Text Input |
| connection_id | str / None | Text Input (optional) |

---

### 🔹 profile_remote_source
| Field Key     | Type        | Field Type |
|---------------|------------|------------|
| uri           | str        | Text Input |
| connection_id | str / None | Text Input (optional) |
| table_filter  | str        | Text Input (comma-separated, optional) |

---

### 🔹 remote_detect_relationships
| Field Key              | Type  | Field Type |
|-----------------------|-------|------------|
| connection_id         | str   | Text Input |
| confidence_threshold  | float | Number Input |

---

### 🔹 remote_enrich_relationships
| Field Key     | Type        | Field Type |
|---------------|------------|------------|
| connection_id | str        | Text Input |
| provider      | str        | Dropdown (`google`, `groq`, `openai`, `anthropic`) |
| model         | str / None | Text Input (optional) |
| incremental   | bool       | Boolean (checkbox) |

---

## 📊 Summary of Field Types

| Field Type            | Description                                      | Count |
|----------------------|--------------------------------------------------|-------|
| Text Input           | Paths, names, queries                            | 34    |
| Sensitive Field      | Credentials, secrets, API keys                   | —     |
| Number Input         | Thresholds, limits, ports                        | 4     |
| Boolean (checkbox)   | Flags (parallel, test, incremental)              | 5     |
| Dropdown / Array     | Providers, schemes, chart types, themes          | 5     |

---

## 📎 Source
Derived from MCP server analysis :contentReference[oaicite:0]{index=0}