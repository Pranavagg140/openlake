pub mod bench;
pub mod cluster;
pub mod disk;
pub mod scrub;
pub mod version;

use anyhow::Result;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum Cmd {
    /// Cluster lifecycle and inspection.
    Cluster(cluster::Args),

    /// Disk inspection commands.
    Disk(disk::Args),

    /// Fabric microbench.
    Bench(bench::Args),

    /// Delete all objects through the public S3 endpoint.
    Scrub(scrub::ScrubArgs),

    Version(version::VersionArgs),
}

pub async fn dispatch(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Cluster(a) => cluster::run(a).await,
        Cmd::Disk(a) => disk::run(a).await,
        Cmd::Bench(a) => bench::run(a).await,
        Cmd::Scrub(a) => scrub::run(a).await,
        Cmd::Version(a) => version::run(a).await,
    }
}