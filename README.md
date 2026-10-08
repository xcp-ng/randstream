# randstream: Reproducible Random Stream Generator and Validator

**`randstream`** is a high-performance command-line utility for creating and
validating reproducible, pseudo-random data streams. It is designed for use
cases such as verifying storage integrity, benchmarking I/O performance, or
generating large, arbitrary datasets for testing.

The utility uses a **seed** to ensure that the generated data is reproducible.
In order to be validatable without regenerating the data, the stream is
processed in chunks (32 KiB by default), and each chunk includes a
**checksum** of 4 bytes at its end for integrity verification.
It also uses **parallel processing** to ensure maximum throughput on modern
hardware, while keeping the output identical independently of the number of
parallel tasks.

## Installation

Download the archive for your platform from the [releases page](./releases).

Or install the binary with `cargo-binstall`:

```bash
cargo binstall randstream
```

Or install from source:

```bash
cargo install randstream
```

## Usage

`randstream` has two main commands: **`generate`** and **`validate`**.

### Generating a Random Stream (`generate`)

Use the generate command to create a reproducible stream of pseudo-random data.

### Validating a Random Stream (`validate`)

Use the `validate` command to verify that an existing stream has not been
corrupted or altered. The validation process checks the **checksum** of each
chunk, so it doesn't need the seed, and reports the chunk where the stream is
corrupted.

Both commands print a **global checksum** of the stream. Pass the one printed
by `generate` to `validate --expected-checksum` to also make sure that the
chunks are the expected ones, in the expected order.

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

**Validate it against the checksum printed by `generate`:**

```bash
randstream validate --expected-checksum 3e6bd002 output.bin
```
