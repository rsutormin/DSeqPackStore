# DSeqPackStore

This package contains the native Rust AGC codec derived from
[ragc at 40e5cad](https://github.com/ekg/ragc/tree/40e5cad) and the incremental
archive writer tested on MHC-10. See [LICENSE](LICENSE) for the upstream license.

The package has one Cargo manifest. The codec lives in `src/`, shared archive
and collection code in `src/common/`, and the CLI in `src/bin/`.

Build from the project root:

```sh
cargo build --release --locked
```

## Local profile store

Use a new or empty directory for the local profile format. A single command can
publish multiple FASTA files in order. Each file is an independent commit.

```sh
target/release/ragc_delta store-publish /tmp/dseq-local reference.fa sample.fa
mkdir -p /tmp/dseq-restored
target/release/ragc_delta store-extract /tmp/dseq-local /tmp/dseq-restored
```

Each profile directory contains its own archive, suffixes, external blocks, and
checkpoint. The top-level `CATALOG` holds committed profile versions, archive
lengths, splitter sets, and sample names. The writer rebuilds the in-memory
inverted index when it opens the store and extends it across a batch.

The command records the target profile before it changes that profile and
publishes the catalog after syncing the data. `store-recover DIR` restores the
profile archive to the version in the catalog. An interrupted first commit
is moved to a hidden orphan directory. Old external pack contents remain
readable up to the published bounds. A retry replaces an unpublished tail
when it next appends to that pack. Store commands take an exclusive local
lock and run recovery on open.

For an existing archive, use `extract` with its original reader and replay
the extracted samples in publication order through `store-publish` into a
separate directory.

## Single archive commands

Create an empty working directory and publish the reference first. Publish subsequent
FASTA files into the same directory using one process at a time:

```sh
mkdir -p /tmp/agc-example
target/release/ragc_delta publish /tmp/agc-example reference.fa
target/release/ragc_delta publish /tmp/agc-example sample.fa
```

The first publication saves a base archive. Each subsequent publication saves a
separate numbered suffix and extends the working archive. The suffix contains new
or changed blocks and an index that points to both new and reused blocks. The
`HEAD` file records the published version and archive length. Run
`ragc_delta recover DIR` to truncate an incomplete write to that length.

Standalone archives can be extracted with `ragc_delta extract ARCHIVE DIR` or stock AGC.
Incremental writes require the checkpoint stream created by this prototype.
Publication assumes a single writer, with readers opening the working archive
after publication.

## Splitter profiles

`publish` and `publish-blocks` automatically select a splitter profile. The
writer samples canonical k-mers, looks up candidates in an inverted index, and
checks their coverage exactly. It creates a new profile if the best existing
profile covers less than 50% of the input and cannot be expanded.

New profiles use binary search in a sorted vector of singleton k-mers by
default. To use the hash-table lookup strategy, pass
`--splitter-candidates=hash` before `store-publish`, `publish`,
`publish-blocks`, or `create`. Both strategies select the same splitters.

```sh
target/release/ragc_delta --splitter-candidates=hash store-publish /tmp/dseq-local sample.fa
```

A profile can be expanded once. The candidate assembly must be at least 30%
longer than the founder, and matching splitters in a consistent order must
cover at least 85% of the founder. The writer discovers extra splitters in
uncovered intervals of at least 100,000 bases that together account for at
least 5% of the input assembly. The existing group IDs and reference segments
remain available to previously written deltas.

New publications write the version 2 append checkpoint. The writer can extend
archives with version 1 checkpoints and rebuilds their profile index from the
stored splitters. The `create` and `append` commands use the initial profile
for fixed-profile encoding.

## External open packs

Use `publish-blocks` for the first publication in a new directory. Later
`publish` calls use the same format automatically.

```sh
mkdir -p /tmp/agc-blocks-example
target/release/ragc_delta publish-blocks /tmp/agc-blocks-example reference.fa
target/release/ragc_delta publish /tmp/agc-blocks-example sample.fa
```

Open packs live in `blocks/`. Each publication appends an independent zstd
frame to each changed open pack. The snapshot records the readable length of
each file. A retry can discard an unpublished tail and append again. A full
pack is written to the main archive, while its previous external file remains
available to earlier snapshots.

Keep `blocks/` next to the archive when copying or replaying its snapshots.
Use the v2 reader for this format. Use regular `publish` from the first
publication to create a standalone archive readable by stock AGC.

For a benchmark, supply individual FASTA files and a list of their file stems in
publication order. Set `RAGC_BENCH_SYNC=fsync` to match the MHC-10 comparison.
The default uses Rust's `File::sync_all()`.

```sh
RAGC_BENCH_SYNC=fsync python3 benchmark_delta.py \
  --input /path/to/fasta --samples /path/to/samples.txt \
  --output /tmp/agc-benchmark
```

Pass `--stock-agc /path/to/agc` to verify every snapshot with the stock reader
as well as the Rust decoder. The benchmark writes a CSV per run.

Pass `--external-blocks` to benchmark external open packs with the v2 reader.
The CSV includes the central archive size and the bytes in block files.
