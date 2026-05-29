pub(crate) struct S3Options {
    pub(crate) endpoint: String,
    pub(crate) bucket: String,
    pub(crate) signer: Option<reqsign_core::Signer<reqsign_aws_v4::Credential>>,
    pub(crate) s3_express_provider: Option<std::sync::Arc<super::fs::S3ExpressCredentialProvider>>,
    pub(crate) s3_express_region: Option<String>,
    pub(crate) sign_payload: bool,
    pub(crate) checksum: bool,
}
