use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::path::PathBuf;

use crate::config;

#[derive(ClapArgs)]
pub struct ScrubArgs {
    /// Cluster TOML. One TOML describes exactly one cluster.
    #[arg(long)]
    pub config: PathBuf,
}

pub async fn run(args: ScrubArgs) -> Result<()> {
    let cfg = config::load(&args.config)
        .with_context(|| format!("load {}", args.config.display()))?;

    if cfg.nodes.is_empty() {
        println!("no openlake cluster detected: {} declares zero nodes", args.config.display());
        return Ok(());
    }

    let mut last_err: Option<anyhow::Error> = None;
    for node in &cfg.nodes {
        let url = format!("http://{}/v1/rpc", node.rpc_addr);
        let client = cyper::Client::builder().http2_prior_knowledge().build();
        let req = openlake_io::rpc::Request::ScrubCluster;
        let body = openlake_io::rpc::encode(&req).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let resp = match client.post(&url)?.body(body).send().await {
            Ok(resp) => resp,
            Err(e) => {
                last_err = Some(anyhow::anyhow!("node {}: {}", node.id, e));
                continue;
            }
        };
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
        if !status.is_success() {
            if let Ok(openlake_io::rpc::Response::Err(e)) = openlake_io::rpc::decode(&bytes) {
                last_err = Some(anyhow::anyhow!("node {}: rpc error: {:?}", node.id, e));
                continue;
            }
            last_err = Some(anyhow::anyhow!("node {}: rpc HTTP status: {}", node.id, status));
            continue;
        }
        match openlake_io::rpc::decode::<openlake_io::rpc::Response>(&bytes)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?
        {
            openlake_io::rpc::Response::Scrub(n) => {
                println!("purged {} objects", n);
                return Ok(());
            }
            openlake_io::rpc::Response::Err(e) => {
                last_err = Some(anyhow::anyhow!("node {}: rpc error: {:?}", node.id, e));
            }
            other => {
                last_err = Some(anyhow::anyhow!("node {}: unexpected rpc response: {:?}", node.id, other));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no openlake cluster node responded")))
}