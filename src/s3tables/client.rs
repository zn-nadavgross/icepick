use crate::s3tables::error::{Result, S3TablesError};

/// Parse S3 Tables ARN and extract region and bucket name
/// ARN format: arn:aws:s3tables:region:account:bucket/name
fn parse_s3tables_arn(arn: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();

    if parts.len() != 6 {
        return Err(S3TablesError::InvalidArn(
            format!("Expected 6 parts, got {}", parts.len())
        ));
    }

    if parts[0] != "arn" {
        return Err(S3TablesError::InvalidArn(
            "Must start with 'arn'".to_string()
        ));
    }

    if parts[2] != "s3tables" {
        return Err(S3TablesError::InvalidArn(
            format!("Not an S3 Tables ARN: {}", parts[2])
        ));
    }

    let region = parts[3].to_string();
    let bucket_name = parts[5]
        .strip_prefix("bucket/")
        .ok_or_else(|| S3TablesError::InvalidArn(
            "Missing 'bucket/' prefix".to_string()
        ))?
        .to_string();

    Ok((region, bucket_name))
}

pub struct S3TablesClient {
    endpoint: String,
    warehouse: String,
    region: String,
}

impl S3TablesClient {
    pub async fn from_arn(arn: &str) -> Result<Self> {
        todo!("implement from_arn")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_arn_valid() {
        let arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_ok());
        let (region, bucket) = result.unwrap();
        assert_eq!(region, "us-west-2");
        assert_eq!(bucket, "my-bucket");
    }

    #[test]
    fn test_parse_arn_invalid_format() {
        let arn = "invalid-arn";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_arn_wrong_service() {
        let arn = "arn:aws:s3:us-west-2:123456789012:bucket/my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_arn_missing_bucket_prefix() {
        let arn = "arn:aws:s3tables:us-west-2:123456789012:my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
    }
}
