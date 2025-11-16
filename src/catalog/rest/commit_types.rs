//! Types for REST catalog commit operations

use serde::{Deserialize, Serialize};

/// Request to commit table changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitTableRequest {
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}

/// Requirements that must be met for commit to succeed
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(clippy::enum_variant_names)]
pub enum TableRequirement {
    #[serde(rename = "assert-current-schema-id")]
    AssertCurrentSchemaId {
        #[serde(rename = "current-schema-id")]
        current_schema_id: i32,
    },

    #[serde(rename = "assert-last-assigned-field-id")]
    AssertLastAssignedFieldId {
        #[serde(rename = "last-assigned-field-id")]
        last_assigned_field_id: i32,
    },

    #[serde(rename = "assert-current-snapshot-id")]
    AssertCurrentSnapshotId {
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
}

/// Updates to apply to the table
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    #[serde(rename = "set-snapshot-ref")]
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        #[serde(rename = "type")]
        ref_type: String,
    },

    #[serde(rename = "upgrade-format-version")]
    UpgradeFormatVersion {
        #[serde(rename = "format-version")]
        format_version: i32,
    },
}

/// Response from commit operation
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct CommitTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    pub metadata: crate::spec::TableMetadata,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_types_serialize() {
        let req = CommitTableRequest {
            requirements: vec![TableRequirement::AssertCurrentSnapshotId {
                snapshot_id: Some(1),
            }],
            updates: vec![TableUpdate::SetSnapshotRef {
                ref_name: "main".to_string(),
                snapshot_id: 2,
                ref_type: "branch".to_string(),
            }],
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("assert-current-snapshot-id"));
        assert!(json.contains("set-snapshot-ref"));
    }
}
