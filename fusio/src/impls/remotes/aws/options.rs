use super::credential::AwsCredential;

pub(crate) struct S3Options {
    pub(crate) endpoint: String,
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) credential: Option<AwsCredential>,
    pub(crate) sign_payload: bool,
    pub(crate) checksum: bool,
}
