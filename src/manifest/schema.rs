//! Avro schemas for Iceberg manifest files (v2 format)

use apache_avro::Schema;

/// Returns the Avro schema for manifest entries in Iceberg v2 format
///
/// # Example
/// ```
/// use icepick::manifest::schema::manifest_entry_schema_v2;
/// let schema = manifest_entry_schema_v2();
/// assert!(schema.is_ok());
/// ```
pub fn manifest_entry_schema_v2() -> Result<Schema, apache_avro::Error> {
    let schema_json = r#"{
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
              "type": "map",
              "values": "string"
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
                "type": "map",
                "values": "long",
                "key-id": 117,
                "value-id": 118
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
                "type": "map",
                "values": "long",
                "key-id": 119,
                "value-id": 120
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
                "type": "map",
                "values": "long",
                "key-id": 121,
                "value-id": 122
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
                "type": "map",
                "values": "bytes",
                "key-id": 126,
                "value-id": 127
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
                "type": "map",
                "values": "bytes",
                "key-id": 129,
                "value-id": 130
              }
            ],
            "default": null,
            "field-id": 124
          },
          {
            "name": "key_metadata",
            "type": ["null", "bytes"],
            "default": null,
            "field-id": 105
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
            "field-id": 106
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

    Schema::parse_str(schema_json)
}

/// Returns the Avro schema for manifest lists in Iceberg v2 format
///
/// # Example
/// ```
/// use icepick::manifest::schema::manifest_list_schema_v2;
/// let schema = manifest_list_schema_v2();
/// assert!(schema.is_ok());
/// ```
pub fn manifest_list_schema_v2() -> Result<Schema, apache_avro::Error> {
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
