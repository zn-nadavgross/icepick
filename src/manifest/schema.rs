//! Avro schemas for Iceberg manifest files (v2 format)

use crate::error::Result;
use crate::manifest::avro::{partition_field_result_type, partition_result_avro_name};
use crate::spec::{PartitionSpec, Schema as IcebergSchema};
use apache_avro::Schema;

/// Returns the Avro schema for manifest entries in Iceberg v2 format.
///
/// The embedded `r102` partition record is generated dynamically — one Avro
/// field per partition spec field, named by field id, typed via the partition
/// transform's result type. Readers (Trino, DuckDB) consult the embedded
/// schema, so each manifest carries the schema it was written against.
pub fn manifest_entry_schema_v2(
    partition_spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
) -> Result<Schema> {
    let partition_fields_json = build_partition_record_fields_json(partition_spec, iceberg_schema)?;

    let schema_json = SCHEMA_TEMPLATE.replace("__PARTITION_FIELDS__", &partition_fields_json);

    Schema::parse_str(&schema_json).map_err(|e| {
        crate::error::Error::invalid_input(format!("Failed to parse manifest entry schema: {}", e))
    })
}

fn build_partition_record_fields_json(
    partition_spec: &PartitionSpec,
    iceberg_schema: &IcebergSchema,
) -> Result<String> {
    let mut parts: Vec<String> = Vec::with_capacity(partition_spec.fields().len());
    for field in partition_spec.fields() {
        let result_type = partition_field_result_type(field, iceberg_schema)?;
        let avro_type = partition_result_avro_name(&result_type)?;
        parts.push(format!(
            r#"{{ "name": "{name}", "type": ["null", "{ty}"], "default": null, "field-id": {id} }}"#,
            name = field.name(),
            ty = avro_type,
            id = field.field_id(),
        ));
    }
    Ok(parts.join(","))
}

const SCHEMA_TEMPLATE: &str = r#"{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    {
      "name": "status",
      "type": "int",
      "field-id": 0,
      "doc": "0=EXISTING, 1=ADDED, 2=DELETED"
    },
    {
      "name": "snapshot_id",
      "type": ["null", "long"],
      "default": null,
      "field-id": 1
    },
    {
      "name": "sequence_number",
      "type": ["null", "long"],
      "default": null,
      "field-id": 3
    },
    {
      "name": "file_sequence_number",
      "type": ["null", "long"],
      "default": null,
      "field-id": 4
    },
    {
      "name": "data_file",
      "type": {
        "type": "record",
        "name": "data_file",
        "fields": [
          {
            "name": "content",
            "type": "int",
            "field-id": 134,
            "doc": "0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES"
          },
          {
            "name": "file_path",
            "type": "string",
            "field-id": 100
          },
          {
            "name": "file_format",
            "type": "string",
            "field-id": 101
          },
          {
            "name": "partition",
            "type": {
              "type": "record",
              "name": "r102",
              "fields": [__PARTITION_FIELDS__]
            },
            "field-id": 102
          },
          {
            "name": "record_count",
            "type": "long",
            "field-id": 103
          },
          {
            "name": "file_size_in_bytes",
            "type": "long",
            "field-id": 104
          },
          {
            "name": "column_sizes",
            "type": [
              "null",
              {
                "type": "array",
                "logicalType": "map",
                "items": {
                  "type": "record",
                  "name": "k117_v118",
                  "fields": [
                    {
                      "name": "key",
                      "type": "int",
                      "field-id": 117
                    },
                    {
                      "name": "value",
                      "type": "long",
                      "field-id": 118
                    }
                  ]
                }
              }
            ],
            "default": null,
            "field-id": 108
          },
          {
            "name": "value_counts",
            "type": [
              "null",
              {
                "type": "array",
                "logicalType": "map",
                "items": {
                  "type": "record",
                  "name": "k119_v120",
                  "fields": [
                    {
                      "name": "key",
                      "type": "int",
                      "field-id": 119
                    },
                    {
                      "name": "value",
                      "type": "long",
                      "field-id": 120
                    }
                  ]
                }
              }
            ],
            "default": null,
            "field-id": 109
          },
          {
            "name": "null_value_counts",
            "type": [
              "null",
              {
                "type": "array",
                "logicalType": "map",
                "items": {
                  "type": "record",
                  "name": "k121_v122",
                  "fields": [
                    {
                      "name": "key",
                      "type": "int",
                      "field-id": 121
                    },
                    {
                      "name": "value",
                      "type": "long",
                      "field-id": 122
                    }
                  ]
                }
              }
            ],
            "default": null,
            "field-id": 110
          },
          {
            "name": "lower_bounds",
            "type": [
              "null",
              {
                "type": "array",
                "logicalType": "map",
                "items": {
                  "type": "record",
                  "name": "k126_v127",
                  "fields": [
                    {
                      "name": "key",
                      "type": "int",
                      "field-id": 126
                    },
                    {
                      "name": "value",
                      "type": "bytes",
                      "field-id": 127
                    }
                  ]
                }
              }
            ],
            "default": null,
            "field-id": 125
          },
          {
            "name": "upper_bounds",
            "type": [
              "null",
              {
                "type": "array",
                "logicalType": "map",
                "items": {
                  "type": "record",
                  "name": "k129_v130",
                  "fields": [
                    {
                      "name": "key",
                      "type": "int",
                      "field-id": 129
                    },
                    {
                      "name": "value",
                      "type": "bytes",
                      "field-id": 130
                    }
                  ]
                }
              }
            ],
            "default": null,
            "field-id": 128
          },
          {
            "name": "key_metadata",
            "type": ["null", "bytes"],
            "default": null,
            "field-id": 131
          },
          {
            "name": "split_offsets",
            "type": [
              "null",
              {
                "type": "array",
                "items": "long",
                "element-id": 133
              }
            ],
            "default": null,
            "field-id": 132
          },
          {
            "name": "equality_ids",
            "type": [
              "null",
              {
                "type": "array",
                "items": "int",
                "element-id": 138
              }
            ],
            "default": null,
            "field-id": 135
          },
          {
            "name": "sort_order_id",
            "type": ["null", "int"],
            "default": null,
            "field-id": 140
          }
        ]
      },
      "field-id": 2
    }
  ]
}"#;

/// Returns the Avro schema for manifest lists in Iceberg v2 format
///
/// # Example
/// ```
/// use icepick::manifest::schema::manifest_list_schema_v2;
/// let schema = manifest_list_schema_v2();
/// assert!(schema.is_ok());
/// ```
pub fn manifest_list_schema_v2() -> std::result::Result<Schema, apache_avro::Error> {
    let schema_json = r#"{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    {
      "name": "manifest_path",
      "type": "string",
      "field-id": 500
    },
    {
      "name": "manifest_length",
      "type": "long",
      "field-id": 501
    },
    {
      "name": "partition_spec_id",
      "type": "int",
      "field-id": 502
    },
    {
      "name": "content",
      "type": "int",
      "field-id": 517,
      "doc": "0=DATA, 1=DELETES"
    },
    {
      "name": "sequence_number",
      "type": "long",
      "field-id": 515
    },
    {
      "name": "min_sequence_number",
      "type": "long",
      "field-id": 516
    },
    {
      "name": "added_snapshot_id",
      "type": "long",
      "field-id": 503
    },
    {
      "name": "added_files_count",
      "type": "int",
      "field-id": 504
    },
    {
      "name": "existing_files_count",
      "type": "int",
      "field-id": 505
    },
    {
      "name": "deleted_files_count",
      "type": "int",
      "field-id": 506
    },
    {
      "name": "added_rows_count",
      "type": "long",
      "field-id": 512
    },
    {
      "name": "existing_rows_count",
      "type": "long",
      "field-id": 513
    },
    {
      "name": "deleted_rows_count",
      "type": "long",
      "field-id": 514
    },
    {
      "name": "partitions",
      "type": [
        "null",
        {
          "type": "array",
          "items": {
            "type": "record",
            "name": "field_summary",
            "fields": [
              {
                "name": "contains_null",
                "type": "boolean",
                "field-id": 509
              },
              {
                "name": "contains_nan",
                "type": ["null", "boolean"],
                "default": null,
                "field-id": 518
              },
              {
                "name": "lower_bound",
                "type": ["null", "bytes"],
                "default": null,
                "field-id": 510
              },
              {
                "name": "upper_bound",
                "type": ["null", "bytes"],
                "default": null,
                "field-id": 511
              }
            ]
          },
          "element-id": 508
        }
      ],
      "default": null,
      "field-id": 507
    },
    {
      "name": "key_metadata",
      "type": ["null", "bytes"],
      "default": null,
      "field-id": 519
    }
  ]
}"#;

    Schema::parse_str(schema_json)
}
