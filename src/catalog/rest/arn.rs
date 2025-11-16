use crate::catalog::{CatalogError, Result};
use percent_encoding::{AsciiSet, CONTROLS};

// Define encoding set for ARN in path
pub const ARN_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// Parse S3 Tables ARN and extract region and bucket name
/// ARN format: arn:aws:s3tables:region:account:bucket/name
pub fn parse_s3tables_arn(arn: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();

    if parts.len() != 6 {
        return Err(CatalogError::InvalidArn(format!(
            "Expected 6 parts, got {}",
            parts.len()
        )));
    }

    if parts[0] != "arn" {
        return Err(CatalogError::InvalidArn(
            "Must start with 'arn'".to_string(),
        ));
    }

    if parts[2] != "s3tables" {
        return Err(CatalogError::InvalidArn(format!(
            "Not an S3 Tables ARN: {}",
            parts[2]
        )));
    }

    let region = parts[3].to_string();
    let bucket_name = parts[5]
        .strip_prefix("bucket/")
        .ok_or_else(|| CatalogError::InvalidArn("Missing 'bucket/' prefix".to_string()))?
        .to_string();

    Ok((region, bucket_name))
}
