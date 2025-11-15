use anyhow::{Context, Result, ensure};

/// Parse S3 Tables ARN and extract region and bucket name
/// ARN format: arn:aws:s3tables:region:account:bucket/name
fn parse_s3_tables_arn(arn: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();
    ensure!(parts.len() == 6, "Invalid S3 Tables ARN format: expected 6 parts");
    ensure!(parts[0] == "arn", "ARN must start with 'arn'");
    ensure!(parts[2] == "s3tables", "Not an S3 Tables ARN");

    let region = parts[3].to_string();
    let bucket_name = parts[5]
        .strip_prefix("bucket/")
        .context("ARN must contain 'bucket/' prefix")?
        .to_string();

    Ok((region, bucket_name))
}

fn main() -> Result<()> {
    // Test ARN parsing
    let test_arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket";
    let (region, bucket) = parse_s3_tables_arn(test_arn)?;
    println!("Region: {}, Bucket: {}", region, bucket);
    Ok(())
}
