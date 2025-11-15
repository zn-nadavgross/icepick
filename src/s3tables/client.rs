use crate::s3tables::error::{Result, S3TablesError};

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
