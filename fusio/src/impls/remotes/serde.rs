use serde::{Deserialize, Serialize};

pub(crate) struct MultipartPart {
    pub part_num: usize,
    pub etag: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
    pub upload_id: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CopyPartResult {
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Default, Debug, Serialize)]
#[serde(default, rename = "CompleteMultipartUpload", rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequest {
    pub part: Vec<CompleteMultipartUploadRequestPart>,
}

#[derive(Clone, Default, Debug, Serialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct CompleteMultipartUploadRequestPart {
    #[serde(rename = "PartNumber")]
    pub part_number: usize,
    #[serde(rename = "ETag")]
    pub etag: String,
}
