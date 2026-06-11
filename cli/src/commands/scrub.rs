use anyhow::{anyhow, Context, Result};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, PercentEncodingMode, SessionTokenMode, SignableBody,
    SignableRequest, SigningSettings, UriPathNormalizationMode,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use base64::Engine as _;
use clap::Args as ClapArgs;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use quick_xml::{de::from_str as xml_from_str, se::to_string as xml_to_string};
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::SystemTime;

use openlake_io::SYSTEM_BUCKET;
use openlake_server::config::{Config, Credential};

const LIST_PAGE_SIZE: u32 = 1000;
const SAMPLE_LIMIT: usize = 10;

#[derive(ClapArgs)]
pub struct ScrubArgs {
    /// Cluster TOML. One TOML describes exactly one cluster.
    #[arg(long)]
    pub config: PathBuf,

    /// Show what would be deleted without performing deletion.
    #[arg(long)]
    pub dry_run: bool,
}

pub async fn run(args: ScrubArgs) -> Result<()> {
    let text = std::fs::read_to_string(&args.config)
        .with_context(|| format!("read {}", args.config.display()))?;
    let cfg =
        Config::from_toml(&text).with_context(|| format!("parse {}", args.config.display()))?;

    if cfg.nodes.is_empty() {
        println!(
            "no openlake cluster detected: {} declares zero nodes",
            args.config.display()
        );
        return Ok(());
    }

    let node = cfg
        .nodes
        .iter()
        .find(|n| n.id == cfg.self_id)
        .context("self_id node missing from config")?;
    let s3_port = cfg.s3_port.unwrap_or_else(|| cfg.s3_addr.port());
    let scheme = if cfg.s3_tls.is_some() {
        "https"
    } else {
        "http"
    };
    let endpoint = SocketAddr::new(node.rpc_addr.ip(), s3_port);
    let base_url = format!("{scheme}://{endpoint}");
    let client = cyper::Client::new();
    let credential = cfg
        .credentials
        .first()
        .context("at least one credential is required")?
        .clone();

    println!("using S3 endpoint on node {}: {}", node.id, base_url);

    let buckets =
        list_bucket_names(&client, &base_url, &cfg.region, &credential, SYSTEM_BUCKET).await?;

    if buckets.is_empty() {
        println!("no buckets discovered through the public S3 endpoint");
        return Ok(());
    }

    if args.dry_run {
        run_dry_run(&client, &base_url, &cfg.region, &credential, &buckets).await?;
        return Ok(());
    }

    let mut total_deleted = 0usize;
    for bucket in &buckets {
        let deleted = scrub_bucket(&client, &base_url, &cfg.region, &credential, bucket).await?;
        total_deleted += deleted;
        println!("purged {} objects from {}", deleted, bucket);
    }

    println!("purged {} objects total", total_deleted);
    Ok(())
}

async fn run_dry_run(
    client: &cyper::Client,
    base_url: &str,
    region: &str,
    credential: &Credential,
    buckets: &[String],
) -> Result<()> {
    println!("WARNING: This operation will delete all objects and may take several minutes.");
    println!("Dry run: no objects will be deleted.");
    println!("Cluster nodes: 1");

    let mut total_objects = 0usize;
    for bucket in buckets {
        let summary = summarize_bucket(client, base_url, region, credential, bucket).await?;
        total_objects += summary.total_objects;
        println!("bucket {}: {} objects", bucket, summary.total_objects);
        if !summary.sample_objects.is_empty() {
            println!("  sample deletions:");
            for key in &summary.sample_objects {
                println!("    {}", key);
            }
        }
    }

    println!("Bucket information was discovered from the public S3 endpoint.");
    println!(
        "Would delete: {} objects across {} buckets.",
        total_objects,
        buckets.len()
    );
    Ok(())
}

struct BucketSummary {
    total_objects: usize,
    sample_objects: Vec<String>,
}

async fn summarize_bucket(
    client: &cyper::Client,
    base_url: &str,
    region: &str,
    credential: &Credential,
    bucket: &str,
) -> Result<BucketSummary> {
    let mut total_objects = 0usize;
    let mut sample_objects = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let page = list_objects_v2(
            client,
            base_url,
            region,
            credential,
            bucket,
            None,
            continuation_token.as_deref(),
            LIST_PAGE_SIZE,
        )
        .await?;

        total_objects += page.contents.len();
        for obj in &page.contents {
            if sample_objects.len() < SAMPLE_LIMIT {
                sample_objects.push(obj.key.clone());
            }
        }

        if !page.is_truncated {
            break;
        }
        continuation_token = page.next_continuation_token;
    }

    Ok(BucketSummary {
        total_objects,
        sample_objects,
    })
}

async fn scrub_bucket(
    client: &cyper::Client,
    base_url: &str,
    region: &str,
    credential: &Credential,
    bucket: &str,
) -> Result<usize> {
    let mut deleted = 0usize;
    let mut continuation_token: Option<String> = None;

    loop {
        let page = list_objects_v2(
            client,
            base_url,
            region,
            credential,
            bucket,
            None,
            continuation_token.as_deref(),
            LIST_PAGE_SIZE,
        )
        .await?;

        let keys: Vec<String> = page.contents.into_iter().map(|obj| obj.key).collect();
        if !keys.is_empty() {
            delete_objects(client, base_url, region, credential, bucket, &keys).await?;
            deleted += keys.len();
        }

        if !page.is_truncated {
            break;
        }
        continuation_token = page.next_continuation_token;
    }

    Ok(deleted)
}

async fn list_bucket_names(
    client: &cyper::Client,
    base_url: &str,
    region: &str,
    credential: &Credential,
    system_bucket: &str,
) -> Result<Vec<String>> {
    let mut buckets = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let page = list_objects_v2(
            client,
            base_url,
            region,
            credential,
            system_bucket,
            Some("buckets/"),
            continuation_token.as_deref(),
            LIST_PAGE_SIZE,
        )
        .await?;

        for obj in page.contents {
            if let Some(bucket) = bucket_name_from_meta_key(&obj.key) {
                if bucket != system_bucket {
                    buckets.push(bucket.to_owned());
                }
            }
        }

        if !page.is_truncated {
            break;
        }
        continuation_token = page.next_continuation_token;
    }

    buckets.sort();
    buckets.dedup();
    Ok(buckets)
}

fn bucket_name_from_meta_key(key: &str) -> Option<&str> {
    key.strip_prefix("buckets/")?.strip_suffix("/.metadata.bin")
}

async fn list_objects_v2(
    client: &cyper::Client,
    base_url: &str,
    region: &str,
    credential: &Credential,
    bucket: &str,
    prefix: Option<&str>,
    continuation_token: Option<&str>,
    max_keys: u32,
) -> Result<ListBucketResult> {
    let url = build_list_url(base_url, bucket, prefix, continuation_token, max_keys);
    let headers = sign_request(
        "GET",
        &url,
        credential,
        region,
        &[],
        SignableBody::UnsignedPayload,
    )?;

    let response = client
        .get(&url)
        .with_context(|| format!("build GET {}", url))?
        .headers(headers)
        .send()
        .await
        .with_context(|| format!("send GET {}", url))?;

    let status = response.status();
    let bytes = response.bytes().await.context("read list response body")?;
    let body = std::str::from_utf8(&bytes).context("list response was not UTF-8")?;

    if !status.is_success() {
        return Err(anyhow!("list {} failed: {}: {}", bucket, status, body));
    }

    let parsed: ListBucketResult = xml_from_str(body).context("parse ListBucketResult")?;
    Ok(parsed)
}

async fn delete_objects(
    client: &cyper::Client,
    base_url: &str,
    region: &str,
    credential: &Credential,
    bucket: &str,
    keys: &[String],
) -> Result<()> {
    let url = format!("{base_url}/{bucket}?delete");
    let body = xml_to_string(&DeleteRequest {
        quiet: Some(true),
        objects: keys
            .iter()
            .map(|key| DeleteRequestObject { key: key.clone() })
            .collect(),
    })
    .context("encode DeleteObjects XML")?;

    let checksum = sha256_base64(body.as_bytes());
    let headers = sign_request(
        "POST",
        &url,
        credential,
        region,
        &[("x-amz-checksum-sha256", checksum.as_str())],
        SignableBody::UnsignedPayload,
    )?;

    let response = client
        .post(&url)
        .with_context(|| format!("build POST {}", url))?
        .headers(headers)
        .body(body)
        .send()
        .await
        .with_context(|| format!("send POST {}", url))?;

    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .context("read delete response body")?;
    let body = std::str::from_utf8(&bytes).context("delete response was not UTF-8")?;

    if !status.is_success() {
        return Err(anyhow!("delete {} failed: {}: {}", bucket, status, body));
    }

    let parsed: DeleteResult = xml_from_str(body).context("parse DeleteResult")?;
    if let Some(err) = parsed.errors.into_iter().next() {
        return Err(anyhow!(
            "delete {} failed for {}: {} ({})",
            bucket,
            err.key,
            err.message,
            err.code
        ));
    }

    Ok(())
}

fn build_list_url(
    base_url: &str,
    bucket: &str,
    prefix: Option<&str>,
    continuation_token: Option<&str>,
    max_keys: u32,
) -> String {
    let mut params = vec!["list-type=2".to_owned(), format!("max-keys={max_keys}")];
    if let Some(prefix) = prefix {
        params.push(format!("prefix={}", encode_query_value(prefix)));
    }
    if let Some(token) = continuation_token {
        params.push(format!("continuation-token={}", encode_query_value(token)));
    }
    format!("{base_url}/{bucket}?{}", params.join("&"))
}

fn encode_query_value(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn sha256_base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(sha2::Sha256::digest(bytes))
}

fn sign_request(
    method: &str,
    url: &str,
    credential: &Credential,
    region: &str,
    extra_headers: &[(&str, &str)],
    body: SignableBody<'static>,
) -> Result<http::HeaderMap> {
    let identity: Identity = Credentials::new(
        &credential.access_key,
        &credential.secret_key,
        None,
        None,
        "openlake-cli",
    )
    .into();

    let mut settings = SigningSettings::default();
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    settings.session_token_mode = SessionTokenMode::Include;

    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .map_err(|e| anyhow!("build signing params: {e}"))?;

    let mut headers: Vec<(&str, &str)> = vec![("host", host_from_url(url))];
    headers.extend_from_slice(extra_headers);

    let signable = SignableRequest::new(method, url, headers.iter().copied(), body)
        .map_err(|e| anyhow!("build signable request: {e}"))?;

    let (instructions, _signature) = sign(signable, &params.into())
        .map_err(|e| anyhow!("sign request: {e}"))?
        .into_parts();

    let mut request = http::Request::builder()
        .method(method)
        .uri(url)
        .header(http::header::HOST, host_from_url(url))
        .body(())
        .map_err(|e| anyhow!("build request: {e}"))?;
    for (name, value) in extra_headers {
        request.headers_mut().insert(
            *name,
            http::HeaderValue::from_str(value).map_err(|e| anyhow!("header {name}: {e}"))?,
        );
    }
    instructions.apply_to_request_http1x(&mut request);

    let mut signed_headers = request.headers().clone();
    signed_headers.remove(http::header::HOST);
    Ok(signed_headers)
}

fn host_from_url(url: &str) -> &str {
    url.split_once("//")
        .and_then(|(_, rest)| rest.split_once('/').map(|(host, _)| host))
        .unwrap_or(url)
}

#[derive(Debug, Deserialize)]
#[serde(rename = "ListBucketResult")]
struct ListBucketResult {
    #[serde(rename = "@xmlns", default)]
    _xmlns: Option<String>,
    #[serde(rename = "Name")]
    _name: String,
    #[serde(rename = "KeyCount", default)]
    _key_count: Option<u32>,
    #[serde(rename = "MaxKeys", default)]
    _max_keys: Option<u32>,
    #[serde(rename = "IsTruncated")]
    is_truncated: bool,
    #[serde(rename = "NextContinuationToken", default)]
    next_continuation_token: Option<String>,
    #[serde(rename = "Contents", default)]
    contents: Vec<ListBucketObject>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "Contents")]
struct ListBucketObject {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "LastModified", default)]
    _last_modified: Option<String>,
    #[serde(rename = "ETag", default)]
    _etag: Option<String>,
    #[serde(rename = "Size", default)]
    _size: Option<u64>,
    #[serde(rename = "StorageClass", default)]
    _storage_class: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename = "Delete")]
struct DeleteRequest {
    #[serde(rename = "Quiet", skip_serializing_if = "Option::is_none")]
    quiet: Option<bool>,
    #[serde(rename = "Object")]
    objects: Vec<DeleteRequestObject>,
}

#[derive(Debug, Serialize)]
#[serde(rename = "Object")]
struct DeleteRequestObject {
    #[serde(rename = "Key")]
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "DeleteResult")]
struct DeleteResult {
    #[serde(rename = "Deleted", default)]
    _deleted: Vec<DeletedEntry>,
    #[serde(rename = "Error", default)]
    errors: Vec<DeleteError>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "Deleted")]
struct DeletedEntry {
    #[serde(rename = "Key")]
    _key: String,
    #[serde(rename = "VersionId", default)]
    _version_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename = "Error")]
struct DeleteError {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId", default)]
    _version_id: Option<String>,
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message")]
    message: String,
}
