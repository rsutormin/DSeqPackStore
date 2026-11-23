import argparse
import csv
import os
import shutil
from pathlib import Path
from statistics import median
from subprocess import run
from tempfile import TemporaryDirectory
from time import perf_counter

parser = argparse.ArgumentParser(description="Benchmark incremental AGC publication on ordered FASTA files.")
parser.add_argument("--input", type=Path, required=True)
parser.add_argument("--samples", type=Path, required=True, help="One FASTA stem per line, with the reference first.")
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--binary", type=Path, default=Path(__file__).parent / "target/release/ragc_delta")
parser.add_argument("--stock-agc", type=Path, help="Also verify every snapshot with a stock AGC executable.")
parser.add_argument("--external-blocks", action="store_true", help="Benchmark append-only external open packs.")
parser.add_argument("--trials", type=int, default=3)
args = parser.parse_args()
if args.external_blocks and args.stock_agc:
    parser.error("External packs require the v2 reader.")
binary = args.binary.resolve()
stock = args.stock_agc.resolve() if args.stock_agc else None
samples = args.samples.read_text().splitlines()
if len(samples) < 2 or len(samples) != len(set(samples)):
    parser.error("Provide at least two distinct sample names.")
inputs = [args.input / f"{sample}.fa" for sample in samples]
results = args.output.resolve()
results.mkdir()

def check(output, expected):
    assert sorted(p.name for p in output.iterdir()) == sorted(p.name for p in expected)
    for source in expected:
        assert (output / source.name).read_bytes() == source.read_bytes(), source.name

for trial in range(1, args.trials + 1):
    directory = results / f'run-{trial}'
    directory.mkdir()
    for parent in (directory, results, results.parent):
        fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    rows = []
    for step, source in enumerate(inputs, 1):
        command = 'publish-blocks' if args.external_blocks and step == 1 else 'publish'
        t = perf_counter()
        result = run([binary, command, directory, source], check=True, capture_output=True, text=True)
        wall = perf_counter() - t
        version, codec, publish, delta, total = result.stdout.strip().split(',')
        assert int(version) == step
        block_bytes = sum(path.stat().st_size for path in (directory / 'blocks').glob('*.zst'))
        rows.append((step, source.stem, codec, publish, wall, delta, total, block_bytes,
                     int(total) + block_bytes))
    with (directory / 'timings.csv').open('w', newline='') as report:
        csv_out = csv.writer(report)
        csv_out.writerow(['step', 'sample', 'codec_s', 'publish_s', 'total_s', 'delta_bytes', 'archive_bytes', 'block_bytes', 'total_stored_bytes'])
        csv_out.writerows(rows)

    with TemporaryDirectory(dir=results) as scratch:
        if args.external_blocks:
            (Path(scratch) / 'blocks').symlink_to(directory / 'blocks', target_is_directory=True)
        replay = Path(scratch) / 'replay.agc'
        shutil.copyfile(directory / '01.base.agc', replay)
        for step in range(1, len(inputs) + 1):
            if step > 1:
                with replay.open('ab') as out, (directory / f'{step:02}.delta').open('rb') as inp:
                    shutil.copyfileobj(inp, out)
            assert replay.stat().st_size == int(rows[step-1][6])
            if stock:
                with TemporaryDirectory(dir=scratch) as output:
                    run([stock, 'getcol', '-t', '1', '-o', output, replay], check=True)
                    check(Path(output), inputs[:step])
            with TemporaryDirectory(dir=scratch) as output:
                run([binary, 'extract', replay, output], check=True)
                check(Path(output), inputs[:step])
        assert replay.read_bytes() == (directory / 'MHC-10.agc').read_bytes()

        interrupted = Path(scratch) / 'interrupted'
        interrupted.mkdir()
        interrupted_archive = interrupted / 'MHC-10.agc'
        shutil.copyfile(directory / '01.base.agc', interrupted_archive)
        with interrupted_archive.open('ab') as out:
            out.write((directory / '02.delta').read_bytes()[:1000])
        (interrupted / 'HEAD').write_text(f'1 {rows[0][6]}\n')
        run([binary, 'recover', interrupted], check=True)
        assert interrupted_archive.read_bytes() == (directory / '01.base.agc').read_bytes()
    timings = [float(row[4]) for row in rows[1:]]
    readers = 'Rust and stock AGC' if stock else 'Rust'
    print(f'Run {trial}: {sum(timings):.5f}s for {len(timings)} appends, median {median(timings):.5f}s, '
          f'{rows[-1][-1]} bytes. All {len(inputs)} snapshots verified by {readers}. Recovery passed.', flush=True)
