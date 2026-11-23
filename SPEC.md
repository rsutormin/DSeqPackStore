# DSeqPackStore

A store for normalized DNA contigs that supports insertion, deletion, versioned snapshots, and selective reads. The segmentation and reference-based encoding ideas come from the AGC paper (DOI 10.1093/bioinformatics/btad097). The store uses its own format.

## Current implementation

- The Rust codec groups segments using reference-derived splitters and encodes
  similar segments as LZ deltas. The decoder supports whole contigs and
  zero-based, half-open ranges.
- The CLI publishes a base archive followed by numbered suffixes. Each suffix
  contains new or changed blocks and an index that also references reused
  blocks. A checkpoint stream stores the information needed to resume encoding.
- Standard archives can be read by stock AGC. Stores created with
  `publish-blocks` keep open packs in separate files beside the archive. The
  writer appends independent zstd frames, and each snapshot records the readable
  length of each file. Full packs are written into the central archive.
- A single writer syncs archive and block data before publishing `HEAD`.
  Recovery truncates the central archive to the published length. Retrying a
  publication can replace unpublished tails of external block files.
- A previous snapshot can be reconstructed by replaying the base archive and
  suffixes through that version. External pack files remain available to
  previous snapshots. The current store provides no garbage collection.

## Target distributed format

- Move the catalog to a transactional store. A single transaction checks the expected HEAD and publishes the snapshot, descriptor versions, request ID, and an indexing event. Upload objects before committing. Use the request ID table to resolve retries after a lost response.
- Store immutable packs in an object store. Resolve each stable record ID through a placement table to an object key, a record index within the pack, and a placement version. Objects can be replicated based on read frequency. References may cross shard boundaries.
- Shard the anchor index by key. Batch lookups by splitter pair first, then by a single boundary and by rare anchors. Check orientation and compression cost. When the middle splitter is missing, evaluate splitting across two bases using the combined cost of the prefix, suffix, and two references. Publish new splitters adaptively for underrepresented regions. Groups serve as search hints.
- Use small, independent zstd frames for short random reads. In the background, group records that have similar bases or are often read together. Publish relocations atomically in the placement table. Delete the old copy after leases on the previous placement version expire.
- Garbage collection marks all retained snapshots, active reads, referenced bases, and staging objects for active uploads. Protect each base before writing its delta and retain that protection until publication. Check the garbage collection generation before physically deleting objects. Snapshot expiration is independent of logical contig deletion. Account for shared bases and backup retention policies when physically deleting objects.

## Verification

Run `cargo test` from the repository root. See [README.md](README.md) for the
incremental benchmark and snapshot verification. To evaluate a distributed
backend, test independent writers, interrupted publications, reclamation races,
and cold-cache p95 latency for 10 kb range reads.
