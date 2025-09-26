# randstream: Reproducible Random Stream Generator and Validator

**`randstream`** is a high-performance command-line utility for creating and
validating reproducible, pseudo-random data streams. It is designed for use
cases such as verifying storage integrity, benchmarking I/O performance, or
generating large, arbitrary datasets for testing.

The utility uses a **seed** to ensure that the generated data is reproducible.
In order to be validatable without regeneration on the data, the stream includes
a **checksum** of 4 bytes at the end of each chunk.
It also uses **parallel processing** to ensure maximum throughput on modern
hardware, while keeping the output identical independently of the number of
parallel tasks.

## Installation

```bash
cargo install randstream
````

## Usage

`randstream` has two main commands: **`generate`** and **`validate`**.

### Generating a Random Stream (`generate`)

Use the generate command to create a reproducible stream of pseudo-random data.

## Validating a Random Stream (`validate`)

Use the `validate` command to verify that an existing stream has not been
corrupted or altered. The validation process will **re-generate** the data
internally using the same seed and compare it byte-for-byte with the input
stream.

### Examples

**Fill a whole block device:**

```bash
randstream generate /dev/xvdb
```

**Generate a 100 GB file using a specific seed and 2 parallel tasks:**

```bash
randstream generate --size 10G --seed 1a234e5678 --jobs 2 output.bin
```

**Validate a previously generated stream:**

```bash
randstream generate output.bin
```

## TODO

- [ ] start position
- [ ] combine the chunk hasher in the thread/global hasher
