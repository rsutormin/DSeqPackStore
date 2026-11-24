use anyhow::{bail, ensure, Context, Result};
use dseqpackstore::common::archive::sync_file as sync;
use dseqpackstore::{
    determine_splitters_streaming, Decompressor, DecompressorConfig, GenomeIO,
    StreamingQueueCompressor, StreamingQueueConfig, CNV_NUM,
};
use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::Path,
    time::Instant,
};

#[path = "ragc_delta/local_store.rs"]
mod local_store;

type ProfileState = (usize, bool, Vec<u64>);

fn encode(output: &Path, previous: Option<&Path>, input: &Path, external_packs: bool, auto_profile: bool, single_profile: bool) -> Result<Option<ProfileState>> {
    let config = StreamingQueueConfig { num_threads: 1, verbosity: 0, ..Default::default() };
    let mut writer = if let Some(source) = previous.filter(|_| auto_profile) {
        StreamingQueueCompressor::append_suffix_auto(output, source, config, input)?
    } else if let Some(source) = previous {
        StreamingQueueCompressor::append_suffix(output, source, config)?
    } else {
        let (splitters, _, _) = determine_splitters_streaming(input, config.k, config.segment_size)?;
        let mut writer = StreamingQueueCompressor::with_splitters(output, config, splitters)?;
        writer.enable_append_checkpoint();
        writer.record_initial_founder(input)?;
        writer
    };
    if external_packs {
        writer.enable_external_packs();
    }
    let sample = input.file_stem().and_then(|name| name.to_str()).context("Invalid sample name")?;
    let mut fasta = GenomeIO::new(File::open(input)?);
    while let Some((header, data)) = fasta.read_contig_converted()? {
        writer.push(sample.to_owned(), header, data)?;
    }
    let profile = single_profile.then(|| writer.single_profile_state()).transpose()?;
    writer.finalize()?;
    Ok(profile)
}

const BLOCK_FORMAT: &str = "blocks/FORMAT";

fn recover(root: &Path) -> Result<(usize, u64)> {
    let archive = root.join("MHC-10.agc");
    let head = root.join("HEAD");
    let data = fs::read_to_string(head)?;
    let mut fields = data.split_whitespace();
    let version = fields.next().context("Missing HEAD version")?.parse()?;
    let size = fields.next().context("Missing HEAD length")?.parse()?;
    ensure!(fields.next().is_none(), "Unexpected HEAD fields");
    truncate_archive(&archive, size)?;
    Ok((version, size))
}

fn truncate_archive(archive: &Path, size: u64) -> Result<()> {
    let file = OpenOptions::new().write(true).open(archive)?;
    let actual = file.metadata()?.len();
    ensure!(actual >= size, "Archive shorter than published HEAD");
    if actual > size {
        file.set_len(size)?;
        sync(&file)?;
    }
    Ok(())
}

struct Publication {
    version: usize,
    encoded_s: f64,
    publish_s: f64,
    delta_bytes: u64,
    archive_bytes: u64,
    profile: Option<ProfileState>,
}

fn publish(root: &Path, input: &Path, request_external_packs: bool, single_profile: bool) -> Result<Publication> {
    let start = Instant::now();
    let archive = root.join("MHC-10.agc");
    let directory = File::open(root)?;
    let marker = root.join(BLOCK_FORMAT);
    let mut external_packs = marker.is_file();
    let has_head = root.join("HEAD").exists();
    if request_external_packs && !external_packs {
        ensure!(!has_head, "External packs require a new store");
        let blocks = root.join("blocks");
        fs::create_dir_all(&blocks)?;
        let format = OpenOptions::new().write(true).create_new(true).open(&marker)?;
        sync(&format)?;
        sync(&File::open(&blocks)?)?;
        sync(&directory)?;
        external_packs = true;
    }
    let (previous, old_len) = if has_head { recover(root)? } else { (0, 0) };
    let version = previous + 1;
    let name = if previous == 0 { "01.base.agc".to_owned() } else { format!("{version:02}.delta") };
    let staged = root.join(format!("{name}.staging"));
    match fs::remove_file(&staged) {
        Err(err) if err.kind() != io::ErrorKind::NotFound => return Err(err.into()),
        _ => {}
    }
    let profile = encode(&staged, (previous != 0).then_some(archive.as_path()), input, external_packs, true, single_profile)?;
    if external_packs {
        sync(&File::open(root.join("blocks"))?)?;
    }
    let encoded = start.elapsed();
    let delta = root.join(name);
    let payload = File::open(&staged)?;
    let delta_len = payload.metadata()?.len();
    sync(&payload)?;
    fs::rename(&staged, &delta)?;
    sync(&directory)?;

    if previous == 0 {
        let staged_archive = root.join("archive.staging");
        fs::copy(&delta, &staged_archive)?;
        sync(&File::open(&staged_archive)?)?;
        fs::rename(staged_archive, &archive)?;
        sync(&directory)?;
    } else {
        let mut target = OpenOptions::new().append(true).open(&archive)?;
        let copied = io::copy(&mut File::open(&delta)?, &mut target)?;
        ensure!(copied == delta_len, "Delta copy length mismatch");
        sync(&target)?;
    }
    let head_stage = root.join("HEAD.staging");
    let mut head = File::create(&head_stage)?;
    writeln!(head, "{version} {}", old_len + delta_len)?;
    sync(&head)?;
    fs::rename(head_stage, root.join("HEAD"))?;
    sync(&directory)?;
    let total = start.elapsed();
    Ok(Publication {
        version,
        encoded_s: encoded.as_secs_f64(),
        publish_s: (total - encoded).as_secs_f64(),
        delta_bytes: delta_len,
        archive_bytes: old_len + delta_len,
        profile,
    })
}

fn extract(archive: &Path, directory: &Path) -> Result<()> {
    let mut reader = Decompressor::open(archive.to_str().context("Invalid archive name")?, DecompressorConfig::default())?;
    for sample in reader.list_samples() {
        let output = directory.join(format!("{sample}.fa"));
        let mut file = BufWriter::new(File::create(output)?);
        for (header, sequence) in reader.get_sample(&sample)? {
            writeln!(file, ">{header}")?;
            for line in sequence.chunks(80) {
                let ascii: Vec<_> = line.iter().map(|&base| CNV_NUM[usize::from(base)]).collect();
                file.write_all(&ascii)?;
                file.write_all(b"\n")?;
            }
        }
        file.flush()?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    rayon::ThreadPoolBuilder::new().num_threads(1).build_global()?;
    match args.as_slice() {
        [mode, out, input] if mode == "create" => encode(Path::new(out), None, Path::new(input), false, false, false).map(|_| ()),
        [mode, source, out, input] if mode == "append" => {
            let source = Path::new(source);
            ensure!(!source.with_file_name(BLOCK_FORMAT).is_file(),
                "Use publish to extend a store with external packs");
            encode(Path::new(out), Some(source), Path::new(input), false, false, false).map(|_| ())
        }
        [mode, root, input] if mode == "publish" || mode == "publish-blocks" => {
            let result = publish(Path::new(root), Path::new(input), mode == "publish-blocks", false)?;
            println!("{},{},{},{},{}", result.version, result.encoded_s, result.publish_s,
                result.delta_bytes, result.archive_bytes);
            Ok(())
        }
        [mode, root, inputs @ ..] if mode == "store-publish" && !inputs.is_empty() => {
            local_store::publish_inputs(Path::new(root), inputs.iter().map(Path::new))
        }
        [mode, root] if mode == "store-recover" => local_store::recover_store(Path::new(root)),
        [mode, root, directory] if mode == "store-extract" => {
            local_store::extract_store(Path::new(root), Path::new(directory))
        }
        [mode, root] if mode == "recover" => recover(Path::new(root)).map(|_| ()),
        [mode, archive, directory] if mode == "extract" => extract(Path::new(archive), Path::new(directory)),
        _ => bail!("usage: ragc_delta create OUT FASTA | append SOURCE SUFFIX FASTA | publish DIR FASTA | publish-blocks DIR FASTA | recover DIR | extract ARCHIVE DIR | store-publish DIR FASTA... | store-recover DIR | store-extract DIR OUT"),
    }
}
