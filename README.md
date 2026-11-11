# randstream: Reproducible Random Stream Generator and Validator

**`randstream`** is a high-performance command-line utility for creating and
validating reproducible, pseudo-random data streams. It is designed for use
cases such as verifying storage integrity, benchmarking I/O performance, or
generating large, arbitrary datasets for testing.

The utility uses a **seed** to ensure that the generated data is reproducible.
In order to be validatable without regeneration on the data, the stream is processed in chunks (32KB by default), and each chunk includes
a **checksum** of 4 bytes at its end for integrity verification.
It also uses **parallel processing** to ensure maximum throughput on modern
hardware, while keeping the output identical independently of the number of
parallel tasks.

## Installation

Download the archive for your platform from the [releases page](./releases).

Or install the binary with `cargo-binstall`:

```bash
cargo binstall ranstream
```

Or install from source:

```bash
cargo install randstream
````

## Usage

`randstream` has three commands: **`generate`**, **`validate`** and
**`stress`**.

### Generating a Random Stream (`generate`)

Use the generate command to create a reproducible stream of pseudo-random data.

When writing to a file or block device, generation is **multithreaded by
default**: the work is split across all physical CPU cores of the host. Use
`--jobs` to set an explicit number of parallel tasks. The output is identical
regardless of the number of tasks. Streaming to standard output is
single-threaded.

### Validating a Random Stream (`validate`)

Use the `validate` command to verify that an existing stream has not been
corrupted or altered. The validation process will **re-generate** the data
internally using the same seed and compare it byte-for-byte with the input
stream.

### Stressing a Device (`stress`)

Use the `stress` command to continuously exercise a file or block device by
generating **and** validating it in a loop, until interrupted with `Ctrl-C`.

The stream is split into fixed-size ranges (1 MiB by default, see
`--range-size`). Every worker thread independently picks a range to either
(re)generate or validate; a range is only ever validated once it has already
been generated, so validation always runs against data that was written first.
Once every range has been generated, a **new random seed** is drawn and the
whole stream is regenerated from scratch, so the device keeps seeing fresh data
over time. The seed is chosen randomly (there is no `--seed` option).

A live indicator reports the current bandwidth, the number of generated and
verified chunks, and how many seeds have been used so far. Use `--no-progress`
to disable it.

### Direct I/O (`--direct`)

All three commands accept `--direct` (`-D`), which opens the file or device
with `O_DIRECT` so reads and writes bypass the kernel page cache — useful to
measure real device throughput or to make sure validation reads the device
rather than cached data. `O_DIRECT` requires block-aligned I/O, so:

- A `--size` written **with a unit** (`5G`, `4Mi`, …) must already be a
  multiple of 4096; otherwise the command fails fast with a clear error,
  rather than silently changing the size you asked for.
- A `--size` given as a **raw byte count** (and a size derived from the file)
  is rounded **down** to the nearest multiple of 4096 automatically.
- The chunk size and position must be multiples of 4096 (error otherwise).

The option is ignored when reading from stdin or writing to stdout.

### I/O size (`--io-size`)

Each thread reads or writes several consecutive chunks at once: `--io-size`
(`-i`, 1 MiB by default) rounded down to a multiple of the chunk size, one chunk
at least. A thread waits for each of its I/Os, so the I/O size and the number of
jobs set how much work the device has in flight: with small I/Os, a device with
a high latency (e.g. a virtual disk) stays mostly idle. The I/O size doesn't
change the stream, nor the validation, which is still done chunk by chunk.

With the default I/O size, a 32 KiB chunk size gives I/Os of 32 chunks, a 4 KiB
one I/Os of 256 chunks, and a chunk size of 1 MiB or more one chunk per I/O.
With `stress`, an I/O doesn't cross a range: `--range-size` limits it too.

The option is ignored when reading from stdin or writing to stdout.

### Examples

**Fill a whole block device:**

```bash
randstream generate /dev/xvdb
```

**Generate a 100 GB file using a specific seed and 2 parallel tasks:**

```bash
randstream generate --size 100G --seed 12345678 --jobs 2 output.bin
```

**Validate a previously generated stream:**

```bash
randstream validate output.bin
```

**Stress a block device until interrupted (`Ctrl-C`):**

```bash
randstream stress /dev/xvdb
```

**Stress a block device for 60 seconds:**

```bash
timeout 60 randstream stress /dev/xvdb
```

**Stress a 50 GB file using 4 threads and 4 MiB ranges:**

```bash
randstream stress --size 50G --jobs 4 --range-size 4Mi stress.bin
```
