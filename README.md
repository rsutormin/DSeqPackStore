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
The prototype uses fixed splitters and assumes a single writer, with readers
opening the working archive after publication.

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
