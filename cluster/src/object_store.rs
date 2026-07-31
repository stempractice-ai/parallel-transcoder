//! S3-compatible object storage client for segment transfer in K8s mode.
//!
//! The cluster's default data plane is SRT (see `srt.rs`) — a good fit for
//! desktop / LAN deployments where nodes stream segments point-to-point.
//! Under Kubernetes, pods come and go and SRT's listen/caller handshake
//! fights the pod lifecycle, so we publish segments to an object store
//! (MinIO in dev, real S3 / GCS / Azure Blob in prod) instead.
//!
//! Protocol compatibility: `SegmentAssignData::srt_url` and
//! `SegmentCompleteData::srt_output_url` carry either `srt://...` or
//! `s3://bucket/key`. The worker inspects the scheme and dispatches to
//! either the SRT or S3 path. Master/worker share the same scheme agreement
//! — no protocol/version bump needed.
//!
//! Credentials: follows the standard AWS credential chain —
//! `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars (how the kind
//! overlay wires MinIO), EKS IRSA, or GKE Workload Identity (how the cloud
//! overlay wires a real bucket).

use std::path::Path;

use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::{
    config::{Builder as S3ConfigBuilder, Region},
    primitives::ByteStream,
    Client,
};

/// A connected S3 client bound to a specific bucket.
#[derive(Clone)]
pub struct ObjectStore {
    client: Client,
    bucket: String,
}

/// Parsed `s3://bucket/key` URI.
pub struct S3Uri<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
}

impl<'a> S3Uri<'a> {
    pub fn parse(uri: &'a str) -> Result<Self> {
        let rest = uri
            .strip_prefix("s3://")
            .ok_or_else(|| anyhow!("not an s3:// URI: {}", uri))?;
        let (bucket, key) = rest
            .split_once('/')
            .ok_or_else(|| anyhow!("s3 URI missing key: {}", uri))?;
        if bucket.is_empty() || key.is_empty() {
            return Err(anyhow!("s3 URI has empty bucket or key: {}", uri));
        }
        Ok(S3Uri { bucket, key })
    }
}

impl ObjectStore {
    /// Connect to an S3-compatible endpoint.
    ///
    /// `endpoint_url` can be a MinIO address (`http://minio.svc:9000`) or a
    /// real S3 endpoint (`https://s3.us-east-1.amazonaws.com`). `bucket` is
    /// the default bucket for uploads; downloads can target any bucket via
    /// a full `s3://` URI.
    pub async fn connect(endpoint_url: &str, bucket: impl Into<String>) -> Result<Self> {
        // The SDK's default region chain falls back to env / config files;
        // MinIO doesn't care about the region but the SDK requires one.
        let region = std::env::var("AWS_REGION")
            .unwrap_or_else(|_| "us-east-1".to_string());

        let base = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(region))
            .load()
            .await;

        let s3_config = S3ConfigBuilder::from(&base)
            .endpoint_url(endpoint_url)
            // MinIO requires path-style addressing (bucket in path, not vhost).
            // Real S3 accepts either, so path-style is the safe default.
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);
        Ok(Self {
            client,
            bucket: bucket.into(),
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Build an `s3://bucket/key` URI pointing at this store's default bucket.
    pub fn build_uri(&self, key: &str) -> String {
        format!("s3://{}/{}", self.bucket, key)
    }

    /// Upload a local file to `s3://<bucket>/<key>`.
    pub async fn upload_file(&self, key: &str, path: &Path) -> Result<()> {
        // ByteStream::from_path streams the body, which makes the SDK
        // default to aws-chunked transfer encoding — OCI's S3-compat
        // endpoint rejects that ("NotImplemented: AWS chunked encoding not
        // supported"). Buffering avoids it entirely; segment files are only
        // a few MB, so this is cheap.
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("failed to read {} for S3 upload", path.display()))?;
        let body = ByteStream::from(bytes);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .with_context(|| format!("S3 PutObject failed for {}/{}", self.bucket, key))?;

        Ok(())
    }

    /// Download an `s3://bucket/key` URI to a local file.
    pub async fn download_to_file(uri: &str, client: &Self, path: &Path) -> Result<()> {
        let s3 = S3Uri::parse(uri)?;
        let resp = client
            .client
            .get_object()
            .bucket(s3.bucket)
            .key(s3.key)
            .send()
            .await
            .with_context(|| format!("S3 GetObject failed for {}", uri))?;

        let data = resp
            .body
            .collect()
            .await
            .context("failed to read S3 object body")?
            .into_bytes();

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        tokio::fs::write(path, &data)
            .await
            .with_context(|| format!("failed to write {}", path.display()))?;
        Ok(())
    }
}

/// True if the URI uses the `s3://` scheme (vs `srt://`).
pub fn is_s3_uri(uri: &str) -> bool {
    uri.starts_with("s3://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_s3_uri() {
        let uri = S3Uri::parse("s3://my-bucket/jobs/abc/seg_0.ts").unwrap();
        assert_eq!(uri.bucket, "my-bucket");
        assert_eq!(uri.key, "jobs/abc/seg_0.ts");
    }

    #[test]
    fn parse_rejects_non_s3() {
        assert!(S3Uri::parse("srt://host:9910").is_err());
        assert!(S3Uri::parse("http://host/path").is_err());
    }

    #[test]
    fn parse_rejects_missing_key() {
        assert!(S3Uri::parse("s3://bucket").is_err());
        assert!(S3Uri::parse("s3://bucket/").is_err());
        assert!(S3Uri::parse("s3:///key").is_err());
    }

    #[test]
    fn is_s3_uri_discriminates_scheme() {
        assert!(is_s3_uri("s3://b/k"));
        assert!(!is_s3_uri("srt://host:9910"));
        assert!(!is_s3_uri(""));
    }
}
