use super::{extract, publish, sync, truncate_archive, SplitterCandidates};
use anyhow::{ensure, Context, Result};
use dseqpackstore::profile_index::SplitterIndex;
use dseqpackstore::{GenomeIO, StreamingQueueCompressor, StreamingQueueConfig};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    time::Instant,
};

const CATALOG: &str = "CATALOG";
const PENDING: &str = "PENDING";
const MAGIC: &[u8] = b"dseq-local-catalog-v1\0";
const MIN_COVERAGE: f64 = 0.50;

#[derive(Default, Deserialize, Serialize)]
struct Catalog {
    profiles: BTreeMap<String, Profile>,
    samples: BTreeMap<String, String>,
}

#[derive(Deserialize, Serialize)]
struct Profile {
    version: usize,
    archive_bytes: u64,
    founder_length: usize,
    expanded: bool,
    splitters: Vec<u64>,
}

struct Store<'a> {
    root: &'a Path,
    _lock: File,
    catalog: Catalog,
}

impl<'a> Store<'a> {
    fn open(root: &'a Path, create: bool) -> Result<Self> {
        if create && !root.exists() {
            fs::create_dir(root)?;
            let parent = root.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
            sync(&File::open(parent)?)?;
        }
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(root.join("LOCK"))?;
        lock.try_lock().with_context(|| format!("Store is already open for publication or recovery: {}", root.display()))?;
        let catalog = match fs::read(root.join(CATALOG)) {
            Ok(bytes) => bincode::deserialize(bytes.strip_prefix(MAGIC).context("Unsupported local store catalog")?)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
                ensure!(!root.join("HEAD").exists(), "Use a new directory for the profile store");
                ensure!(fs::read_dir(root)?.all(|e| e.is_ok_and(|e| {
                    e.file_name() == "LOCK" || e.file_name() == "CATALOG.staging"
                })), "Expected an empty directory for the profile store");
                let empty = Catalog::default();
                write_catalog(root, &empty)?;
                empty
            }
            Err(error) => return Err(error).context("Open local store catalog"),
        };
        let store = Self { root, _lock: lock, catalog };
        store.recover()?;
        Ok(store)
    }

    fn recover(&self) -> Result<()> {
        let name = match fs::read_to_string(self.root.join(PENDING)) {
            Ok(name) => name,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error).context("Read pending profile"),
        };
        ensure!(name.strip_prefix('p').is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit())),
            "Invalid pending profile name");
        let profile_path = self.root.join(&name);
        if let Some(profile) = self.catalog.profiles.get(&name) {
            // The catalog is authoritative even if the profile HEAD was advanced.
            truncate_archive(&profile_path.join("MHC-10.agc"), profile.archive_bytes)?;
            write_atomic(&profile_path, "HEAD", format!("{} {}\n", profile.version, profile.archive_bytes).as_bytes())?;
        } else if profile_path.exists() {
            // Preserve an interrupted first publication without exposing it in the catalog.
            let path = orphan_path(self.root, &name);
            fs::rename(&profile_path, path)?;
            sync(&File::open(self.root)?)?;
        }
        self.clear_pending()
    }

    fn clear_pending(&self) -> Result<()> {
        fs::remove_file(self.root.join(PENDING))?;
        sync(&File::open(self.root)?)?;
        Ok(())
    }

    fn add(&mut self, index: &mut SplitterIndex, input: &Path, candidates: SplitterCandidates) -> Result<()> {
        let start = Instant::now();
        let sample = input.file_stem().and_then(|s| s.to_str()).context("Invalid sample name")?;
        ensure!(!self.catalog.samples.contains_key(sample), "Sample already exists: {sample}");
        let config = StreamingQueueConfig::default();
        let mut fasta = GenomeIO::new(File::open(input)?);
        let mut contigs = Vec::new();
        while let Some((_, contig)) = fasta.read_contig_converted()? {
            contigs.push(contig);
        }
        ensure!(!contigs.is_empty(), "FASTA contains no contigs");
        let total_nt: usize = contigs.iter().map(Vec::len).sum();
        let matched = index.select_two_stage(contigs.iter().map(Vec::as_slice), config.k, config.segment_size, MIN_COVERAGE);
        let mut selected = None;
        if let Some(candidate) = matched.name {
            let profile = &self.catalog.profiles[&candidate];
            let minimum = profile.founder_length.saturating_add(profile.founder_length.saturating_mul(3).div_ceil(10));
            if matched.coverage >= MIN_COVERAGE || (!profile.expanded && total_nt >= minimum
                && StreamingQueueCompressor::can_expand_profile(
                    &self.root.join(&candidate).join("MHC-10.agc"), input, config,
                )?) {
                selected = Some(candidate);
            }
        }
        drop(contigs);
        let new_profile = selected.is_none();
        let name = selected.unwrap_or_else(|| format!("p{:04}", self.catalog.profiles.len() + 1));
        let target = self.root.join(&name);
        let search = start.elapsed().as_secs_f64();
        if new_profile {
            ensure!(!target.exists(), "Profile directory already exists: {}", target.display());
        }
        write_atomic(self.root, PENDING, name.as_bytes())?;
        if new_profile {
            fs::create_dir(&target)?;
            sync(&File::open(self.root)?)?;
        }
        let result = publish(&target, input, new_profile, true, candidates)?;
        let (founder_length, expanded, splitters) = result.profile.context("Missing profile state")?;
        if let Some(previous) = self.catalog.profiles.get(&name) {
            let new_keys: Vec<_> = splitters.iter().copied()
                .filter(|key| previous.splitters.binary_search(key).is_err()).collect();
            if !new_keys.is_empty() {
                index.extend_profile(&name, &new_keys);
            }
        } else {
            index.add_profile(name.clone(), &splitters);
        }
        self.catalog.profiles.insert(name.clone(), Profile {
            version: result.version, archive_bytes: result.archive_bytes, founder_length, expanded, splitters,
        });
        self.catalog.samples.insert(sample.to_owned(), name.clone());
        write_catalog(self.root, &self.catalog)?;
        self.clear_pending()?;
        println!("{sample},{name},{},{search:.9},{:.9},{},{:.9}",
            result.version, result.encoded_s, result.delta_bytes, start.elapsed().as_secs_f64());
        Ok(())
    }
}

fn orphan_path(root: &Path, profile: &str) -> PathBuf {
    (1..).map(|n| root.join(format!(".orphan-{profile}-{n}")))
        .find(|path| !path.exists()).expect("No available orphan filename")
}

fn write_catalog(root: &Path, catalog: &Catalog) -> Result<()> {
    let mut bytes = MAGIC.to_vec();
    bincode::serialize_into(&mut bytes, catalog)?;
    Ok(write_atomic(root, CATALOG, &bytes)?)
}

fn write_atomic(root: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let staging = root.join(format!("{name}.staging"));
    let mut file = File::create(&staging)?;
    file.write_all(bytes)?;
    sync(&file)?;
    fs::rename(staging, root.join(name))?;
    sync(&File::open(root)?)
}

pub(super) fn publish_inputs<'a>(
    root: &Path,
    inputs: impl IntoIterator<Item = &'a Path>,
    candidates: SplitterCandidates,
) -> Result<()> {
    let mut store = Store::open(root, true)?;
    let mut index = SplitterIndex::default();
    for (name, profile) in &store.catalog.profiles {
        index.add_profile(name.clone(), &profile.splitters);
    }
    for input in inputs {
        store.add(&mut index, input, candidates)?;
    }
    Ok(())
}

pub(super) fn recover_store(root: &Path) -> Result<()> {
    Store::open(root, false).map(|_| ())
}

pub(super) fn extract_store(root: &Path, output: &Path) -> Result<()> {
    let store = Store::open(root, false)?;
    for name in store.catalog.profiles.keys() {
        extract(&root.join(name).join("MHC-10.agc"), output)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_rolls_back_an_unpublished_profile_version() -> Result<()> {
        let workspace = tempfile::tempdir()?;
        let root = workspace.path().join("store");
        let original = workspace.path().join("original.fa");
        let next = workspace.path().join("next.fa");
        let sequence = (0..140_000).scan(1u32, |state, _| {
            *state ^= *state << 13;
            *state ^= *state >> 17;
            *state ^= *state << 5;
            Some(b"ACGT"[(*state & 3) as usize])
        }).collect::<Vec<_>>();
        let mut fasta = b">contig\n".to_vec();
        for line in sequence.chunks(80) {
            fasta.extend_from_slice(line);
            fasta.push(b'\n');
        }
        fs::write(&original, &fasta)?;
        fs::write(&next, &fasta)?;
        publish_inputs(&root, [&original as &Path], SplitterCandidates::Sorted)?;

        let shard = root.join("p0001");
        let committed_head = fs::read(shard.join("HEAD"))?;
        let committed_length = fs::metadata(shard.join("MHC-10.agc"))?.len();
        write_atomic(&root, PENDING, b"p0001")?;
        let unpublished = publish(&shard, &next, false, true, SplitterCandidates::Sorted)?;
        assert_eq!(unpublished.version, 2);
        assert!(unpublished.archive_bytes > committed_length);
        recover_store(&root)?;
        assert!(!root.join(PENDING).exists());
        assert_eq!(fs::read(shard.join("HEAD"))?, committed_head);
        assert_eq!(fs::metadata(shard.join("MHC-10.agc"))?.len(), committed_length);

        let before = workspace.path().join("before");
        fs::create_dir(&before)?;
        extract_store(&root, &before)?;
        assert_eq!(fs::read(before.join("original.fa"))?, fasta);
        assert!(!before.join("next.fa").exists());

        publish_inputs(&root, [&next as &Path], SplitterCandidates::Hash)?;
        // A crash after publishing the catalog leaves a harmless pending marker.
        write_atomic(&root, PENDING, b"p0001")?;
        let after = workspace.path().join("after");
        fs::create_dir(&after)?;
        extract_store(&root, &after)?;
        assert_eq!(fs::read(after.join("original.fa"))?, fasta);
        assert_eq!(fs::read(after.join("next.fa"))?, fasta);
        assert!(publish_inputs(&root, [&next as &Path], SplitterCandidates::Hash).is_err());
        Ok(())
    }

    #[test]
    fn recovery_quarantines_an_unpublished_profile() -> Result<()> {
        let root = tempfile::tempdir()?;
        let locked = Store::open(root.path(), true)?;
        assert!(Store::open(root.path(), false).is_err());
        drop(locked);
        write_atomic(root.path(), PENDING, b"p0001")?;
        fs::create_dir(root.path().join("p0001"))?;
        recover_store(root.path())?;
        assert!(!root.path().join("p0001").exists());
        assert!(root.path().join(".orphan-p0001-1").is_dir());
        assert!(!root.path().join(PENDING).exists());
        recover_store(root.path())?;
        Ok(())
    }
}
