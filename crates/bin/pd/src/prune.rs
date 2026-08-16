//! In-place pruning of the Jellyfish Merkle Tree in the main store.
//!
//! Adapted from penumbra-zone/penumbra#5272 (`pd migrate prune`) onto v2.0.0
//! as a standalone subcommand. cnidarium's `prune_main_substore` streams every
//! key/value at the latest version into a fresh database, verifying range
//! proofs so the root hash cannot change; auxiliary column families and the
//! substores are copied byte for byte. The pruned database is a normal
//! cnidarium database, so stock `pd` reads it. Only historical-version queries
//! (which pd never issues) are lost.

use std::path::{Path, PathBuf};

use anyhow::Result;
use cnidarium::{prune_main_substore, PruneConfig, Storage};
use penumbra_sdk_app::SUBSTORE_PREFIXES;
use rocksdb::DB;

fn copy_column_family(old_db: &DB, new_db: &DB, cf_name: &str) -> Result<u64> {
    let old_cf = old_db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow::anyhow!("column family '{}' not found in old database", cf_name))?;
    let new_cf = new_db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow::anyhow!("column family '{}' not found in new database", cf_name))?;

    let mut count = 0u64;
    let mut batch = rocksdb::WriteBatch::default();
    let mut iter = old_db.raw_iterator_cf(old_cf);
    iter.seek_to_first();
    while iter.valid() {
        if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
            batch.put_cf(new_cf, key, value);
            count += 1;
            if count % 10_000 == 0 {
                new_db.write(std::mem::take(&mut batch))?;
            }
        }
        iter.next();
    }
    if !batch.is_empty() {
        new_db.write(batch)?;
    }
    Ok(count)
}

fn dir_size(path: &Path) -> u64 {
    fn walk(path: &Path) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    total += walk(&p);
                } else if let Ok(md) = entry.metadata() {
                    total += md.len();
                }
            }
        }
        total
    }
    walk(path)
}

/// Prune the main store JMT under `pd_home/rocksdb` in place.
pub async fn prune_state(pd_home: PathBuf, chunk_size: usize) -> Result<()> {
    let rocksdb_dir = pd_home.join("rocksdb");
    let rocksdb_new = pd_home.join("rocksdb_new");
    let rocksdb_old = pd_home.join("rocksdb_old");

    let initial_size = dir_size(&rocksdb_dir);
    tracing::info!(initial_size_bytes = initial_size, "rocksdb directory size before pruning");

    let storage = Storage::load(rocksdb_dir.clone(), SUBSTORE_PREFIXES.clone()).await?;
    let snapshot = storage.latest_snapshot();
    let original_root_hash = snapshot.root_hash().await?;
    let version = snapshot.version();
    tracing::info!(?original_root_hash, version, "starting JMT pruning");

    let db = storage.db();

    if rocksdb_new.exists() {
        std::fs::remove_dir_all(&rocksdb_new)?;
    }
    if rocksdb_old.exists() {
        std::fs::remove_dir_all(&rocksdb_old)?;
    }

    tracing::info!("creating fresh database at {:?}", rocksdb_new);
    let new_storage = Storage::load(rocksdb_new.clone(), SUBSTORE_PREFIXES.clone()).await?;
    let new_db = new_storage.db();

    let prune_config = PruneConfig {
        chunk_size,
        ..Default::default()
    };
    tracing::info!(chunk_size, "pruning main store");
    let report = prune_main_substore(&storage, snapshot, &new_storage, version, &prune_config)?;
    tracing::info!(
        keys_processed = report.keys_processed,
        nodes_before = report.nodes_before,
        nodes_after = report.nodes_after,
        "main store pruned (root hash verified via range proofs)"
    );

    tracing::info!("copying auxiliary column families from old database");
    for cf_name in [
        "config",
        "substore--jmt-keys",
        "substore--jmt-keys-by-keyhash",
        "substore--nonverifiable",
    ] {
        let count = copy_column_family(&db, &new_db, cf_name)?;
        tracing::info!(cf_name, count, "copied column family");
    }

    tracing::info!("copying substore column families");
    for prefix in SUBSTORE_PREFIXES.iter() {
        for cf_name in [
            format!("substore-{}-jmt", prefix),
            format!("substore-{}-jmt-keys", prefix),
            format!("substore-{}-jmt-values", prefix),
            format!("substore-{}-jmt-keys-by-keyhash", prefix),
            format!("substore-{}-nonverifiable", prefix),
        ] {
            let count = copy_column_family(&db, &new_db, &cf_name)?;
            tracing::info!(cf_name, count, "copied column family");
        }
    }

    // Verify the new database independently before swapping anything.
    drop(new_db);
    drop(db);
    new_storage.release().await;
    storage.release().await;
    let check = Storage::load(rocksdb_new.clone(), SUBSTORE_PREFIXES.clone()).await?;
    let check_snapshot = check.latest_snapshot();
    let new_root_hash = check_snapshot.root_hash().await?;
    let new_version = check_snapshot.version();
    check.release().await;
    anyhow::ensure!(
        new_root_hash == original_root_hash && new_version == version,
        "pruned database does not match: root {:?} vs {:?}, version {} vs {}",
        new_root_hash,
        original_root_hash,
        new_version,
        version
    );
    tracing::info!(?new_root_hash, new_version, "pruned database verified");

    tracing::info!("swapping database directories");
    std::fs::rename(&rocksdb_dir, &rocksdb_old)?;
    std::fs::rename(&rocksdb_new, &rocksdb_dir)?;
    tracing::info!("removing old database");
    std::fs::remove_dir_all(&rocksdb_old)?;

    for entry in std::fs::read_dir(&rocksdb_dir)? {
        let entry = entry?;
        if let Some(name) = entry.file_name().to_str() {
            if name.starts_with("LOG.old") {
                std::fs::remove_file(entry.path())?;
            }
        }
    }

    let final_size = dir_size(&rocksdb_dir);
    tracing::info!(
        "pruning complete: {:.1} GB -> {:.1} GB",
        initial_size as f64 / 1e9,
        final_size as f64 / 1e9,
    );
    Ok(())
}
