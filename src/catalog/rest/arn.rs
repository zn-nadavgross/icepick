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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3tables_arn_valid() {
        let arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_ok());
        let (region, bucket) = result.unwrap();
        assert_eq!(region, "us-west-2");
        assert_eq!(bucket, "my-bucket");
    }

    #[test]
    fn test_parse_s3tables_arn_different_regions() {
        let test_cases = vec![
            (
                "arn:aws:s3tables:us-east-1:123456789012:bucket/test",
                "us-east-1",
                "test",
            ),
            (
                "arn:aws:s3tables:eu-west-1:999999999999:bucket/prod-bucket",
                "eu-west-1",
                "prod-bucket",
            ),
            (
                "arn:aws:s3tables:ap-southeast-2:111111111111:bucket/my-data",
                "ap-southeast-2",
                "my-data",
            ),
        ];

        for (arn, expected_region, expected_bucket) in test_cases {
            let result = parse_s3tables_arn(arn).expect("ARN should be valid");
            assert_eq!(result.0, expected_region);
            assert_eq!(result.1, expected_bucket);
        }
    }

    #[test]
    fn test_parse_s3tables_arn_invalid_format() {
        let arn = "invalid-arn";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CatalogError::InvalidArn(_)));
    }

    #[test]
    fn test_parse_s3tables_arn_wrong_service() {
        let arn = "arn:aws:s3:us-west-2:123456789012:bucket/my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CatalogError::InvalidArn(_)));
        assert!(err.to_string().contains("s3"));
    }

    #[test]
    fn test_parse_s3tables_arn_missing_bucket_prefix() {
        let arn = "arn:aws:s3tables:us-west-2:123456789012:my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CatalogError::InvalidArn(_)));
        assert!(err.to_string().contains("bucket/"));
    }

    #[test]
    fn test_parse_s3tables_arn_too_few_parts() {
        let arn = "arn:aws:s3tables:us-west-2";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CatalogError::InvalidArn(_)));
        assert!(err.to_string().contains("6 parts"));
    }

    #[test]
    fn test_parse_s3tables_arn_not_starting_with_arn() {
        let arn = "aws:aws:s3tables:us-west-2:123456789012:bucket/my-bucket";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CatalogError::InvalidArn(_)));
        assert!(err.to_string().contains("arn"));
    }

    #[test]
    fn test_parse_s3tables_arn_bucket_name_with_hyphens() {
        let arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-test-bucket-123";
        let result = parse_s3tables_arn(arn);
        assert!(result.is_ok());
        let (_, bucket) = result.unwrap();
        assert_eq!(bucket, "my-test-bucket-123");
    }
}
