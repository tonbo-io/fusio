#[derive(Debug, Clone)]
pub struct AwsCredential {
    /// AWS_ACCESS_KEY_ID
    pub key_id: String,
    /// AWS_SECRET_ACCESS_KEY
    pub secret_key: String,
    /// AWS_SESSION_TOKEN
    pub token: Option<String>,
}
