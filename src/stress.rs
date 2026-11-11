use aligned_vec::{AVec, RuntimeAlign};
use anyhow::anyhow;
use clap::Args;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use itertools::Itertools as _;
use log::{debug, info};
use parse_size::parse_size;
use rand::{Rng, RngExt as _};
use std::fs::OpenOptions;
use std::io::IsTerminal as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::cli::CommonArgs;
use crate::generate::{StreamParams, ThreadWork, write_chunk_range};
use crate::validate::validate_chunk_range;
use crate::{
    DIRECT_ALIGN, check_direct_alignment, direct_aligned_stream_size, log_metrics, read_file_size,
};

/// Endlessly generate and validate a random stream
///
/// The stream is split into fixed-size ranges (1 MiB by default). A shared map
/// tracks which ranges have already been generated. Every worker thread keeps
/// picking work on its own: it either (re)generates a range, or validates a
/// range that has already been generated. A range that is never generated is
/// never validated, so validation always runs on data that was written first.
///
/// The loop runs until interrupted (Ctrl-C). Any checksum mismatch aborts with
/// an error.
#[derive(Args, Debug)]
#[command(alias = "torture")]
pub struct StressArgs {
    /// The file or block device to stress
    #[arg()]
    pub file: PathBuf,

    /// The stream position
    #[clap(short, long, default_value = "0", value_parser=|s: &str| parse_size(s))]
    pub position: u64,

    /// The size of a range, the unit of work picked by each thread
    #[clap(short, long, default_value = "1Mi", value_parser=|s: &str| parse_size(s))]
    pub range_size: u64,

    #[clap(flatten)]
    pub common: CommonArgs,
}

#[derive(Clone, Copy)]
enum Action {
    Generate,
    Validate,
}

struct Job {
    range: u64,
    action: Action,
    /// Seed of the epoch this job belongs to (used by generation).
    seed: u64,
    /// Epoch this job was claimed in, so a late-finishing job from a previous
    /// epoch is not miscounted against the current one.
    epoch: u64,
    /// True when this generate job is the first generation of this range in its
    /// epoch (the range was `Empty` when claimed).
    first_time: bool,
}

/// Mutable scheduler state guarded by a single mutex.
///
/// Every range index is in exactly one place at any time: in `empty` (needs
/// (re)generation for the current epoch), in `generated` (written this epoch,
/// idle and validatable), or in neither (a worker holds it — "busy"). This
/// makes [`Scheduler::claim`] O(1) instead of an O(num_ranges) scan.
struct SchedState {
    /// Ranges that still need (re)generation for the current epoch.
    empty: Vec<usize>,
    /// Ranges generated this epoch, idle and safe to validate or regenerate.
    generated: Vec<usize>,
    num_ranges: usize,
    /// Seed used to generate ranges during the current epoch.
    seed: u64,
    epoch: u64,
    /// Number of first-time range generations still needed before every range
    /// has been written once this epoch.
    remaining: usize,
}

/// Hands out generate/validate jobs while enforcing that a range is validated
/// only once it has been generated and is not currently being written. Once
/// every range has been generated in the current epoch, a fresh seed is drawn
/// and the generated set is cleared so the whole stream is regenerated anew.
struct Scheduler {
    inner: Mutex<SchedState>,
    generated: AtomicU64,
    validated: AtomicU64,
    /// Number of distinct seeds generated so far (the initial one included).
    seeds: AtomicU64,
}

impl Scheduler {
    fn new(num_ranges: usize, seed: u64) -> Self {
        Scheduler {
            inner: Mutex::new(SchedState {
                empty: (0..num_ranges).collect(),
                generated: Vec::new(),
                num_ranges,
                seed,
                epoch: 0,
                remaining: num_ranges,
            }),
            generated: AtomicU64::new(0),
            validated: AtomicU64::new(0),
            seeds: AtomicU64::new(1),
        }
    }

    /// Pick a job. Validation is only ever offered for a range in the
    /// `generated` set, which guarantees it was fully written beforehand and is
    /// not being rewritten concurrently. The chosen range is removed from its
    /// set (it becomes "busy" until [`Scheduler::finish`] returns it). Returns
    /// `None` when every range is busy (the caller should back off and retry).
    fn claim(&self, rng: &mut impl Rng) -> Option<Job> {
        let mut st = self.inner.lock().unwrap();
        if st.empty.is_empty() && st.generated.is_empty() {
            return None;
        }

        // Validate only if something has been generated; otherwise generate.
        // When both are possible, flip a coin to keep mixing the workload.
        // `swap_remove` at a uniformly random index is an O(1) random pick.
        let validate = !st.generated.is_empty() && (st.empty.is_empty() || rng.random_bool(0.5));
        let (range, action, first_time) = if validate {
            let i = rng.random_range(0..st.generated.len());
            (st.generated.swap_remove(i), Action::Validate, false)
        } else if !st.empty.is_empty() {
            let i = rng.random_range(0..st.empty.len());
            (st.empty.swap_remove(i), Action::Generate, true)
        } else {
            let i = rng.random_range(0..st.generated.len());
            (st.generated.swap_remove(i), Action::Generate, false)
        };

        Some(Job { range: range as u64, action, seed: st.seed, epoch: st.epoch, first_time })
    }

    /// Return a range to the map after work completed. When the final first-time
    /// generation of an epoch completes, a new seed is drawn and every range
    /// (including those currently busy) must be regenerated before the next
    /// rotation, so the whole stream is rewritten with the new seed.
    fn finish(&self, job: &Job, success: bool) {
        let mut rotated_seed = None;
        {
            let mut st = self.inner.lock().unwrap();
            let range = job.range as usize;
            if job.epoch != st.epoch {
                // The job belongs to a finished epoch: whatever it wrote (or
                // read) is keyed to an old seed and is now stale. Return the
                // range to `empty` so it is regenerated with the current seed,
                // and leave `remaining` untouched — a stale range still counts
                // as not-yet-(re)generated for the current epoch.
                st.empty.push(range);
            } else {
                match job.action {
                    Action::Validate => st.generated.push(range),
                    Action::Generate if !success => {
                        // Incomplete data: keep it out of the validatable set.
                        st.empty.push(range);
                    }
                    Action::Generate => {
                        st.generated.push(range);
                        // Only the first-time generation of a range counts
                        // towards completing the epoch (the job's epoch matches
                        // the current one in this branch).
                        if job.first_time {
                            st.remaining -= 1;
                            if st.remaining == 0 {
                                let new_seed = rand::rng().random::<u64>();
                                st.seed = new_seed;
                                st.epoch += 1;
                                // Every range must be rewritten with the new
                                // seed: move all generated ranges back to
                                // `empty`. Busy ones join it via the stale-epoch
                                // branch above when their old-epoch job ends.
                                let mut prev = std::mem::take(&mut st.generated);
                                st.empty.append(&mut prev);
                                st.remaining = st.num_ranges;
                                rotated_seed = Some(new_seed);
                            }
                        }
                    }
                }
            }
        }
        if let Some(seed) = rotated_seed {
            self.seeds.fetch_add(1, Ordering::Relaxed);
            debug!("every range generated; seed rotated to {seed}");
        }
    }
}

pub fn stress(args: &StressArgs, cancel: Arc<AtomicBool>) -> anyhow::Result<i32> {
    let start = Instant::now();
    let chunk_size = args.common.chunk_size as usize;
    // we need to write a multiple of 64 bits to be able to use advance()
    let buffer_size = chunk_size.div_ceil(8) * 8;
    let stream_size = resolve_stream_size(args)?;
    if args.common.direct {
        check_direct_alignment(chunk_size as u64, args.position)?;
    }
    // Pick a fresh random seed for the first epoch; the scheduler rotates it
    // every time the whole stream has been generated.
    let seed = rand::rng().random::<u64>();

    let num_threads = args.common.jobs.unwrap_or(num_cpus::get_physical());
    let num_chunks = stream_size.div_ceil(chunk_size as u64);
    let chunks_per_range = (args.range_size.div_ceil(chunk_size as u64)).max(1);
    let num_ranges = num_chunks.div_ceil(chunks_per_range);

    // make sure the file exists and is large enough before the threads open it
    let f = OpenOptions::new().create(true).truncate(false).write(true).open(&args.file)?;
    if args.file.is_file() {
        let end_position = stream_size + args.position;
        if end_position > f.metadata()?.len() {
            f.set_len(end_position)?;
        }
    }
    drop(f);

    debug!("number of threads: {num_threads}");
    debug!("seed: {seed}");
    debug!("chunk size: {chunk_size}");
    debug!("position: {}", args.position);
    debug!("range size: {} ({chunks_per_range} chunks)", chunks_per_range * chunk_size as u64);
    debug!("number of ranges: {num_ranges}");

    let position = args.position;
    let direct = args.common.direct;
    let io_chunks = args.common.io_chunks();
    let scheduler = Arc::new(Scheduler::new(num_ranges as usize, seed));
    // write_chunk_range()/validate_chunk_range() report progress through this
    // channel. A drain thread keeps it emptied, sums the bytes processed, and
    // renders a live spinner with the current bandwidth and chunk counts.
    let (tx, rx) = mpsc::channel::<u64>();
    let total_bytes = Arc::new(AtomicU64::new(0));
    let drain = spawn_progress(rx, total_bytes.clone(), scheduler.clone(), args.common.no_progress);

    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let file = args.file.clone();
            let scheduler = scheduler.clone();
            let cancel = cancel.clone();
            let tx = tx.clone();
            let stream = StreamParams {
                seed: 0,
                position,
                stream_size,
                chunk_size,
                buffer_size,
                io_chunks,
                direct,
            };
            thread::spawn(move || {
                let ctx = WorkerCtx {
                    file: &file,
                    stream,
                    num_chunks,
                    chunks_per_range,
                    scheduler: &scheduler,
                    tx: &tx,
                    cancel: &cancel,
                };
                worker(&ctx)
            })
        })
        .collect();

    // Drop our channel handles so the drain thread can finish once every worker
    // thread has exited and dropped its own clone.
    drop(tx);
    let result: anyhow::Result<()> = handles.into_iter().map(|h| h.join().unwrap()).try_collect();
    drain.join().unwrap();

    let total_bytes = total_bytes.load(Ordering::Relaxed);
    info!(
        "generated {} chunk(s), validated {} chunk(s), used {} seed(s)",
        scheduler.generated.load(Ordering::Relaxed),
        scheduler.validated.load(Ordering::Relaxed),
        scheduler.seeds.load(Ordering::Relaxed)
    );
    log_metrics(start, total_bytes, "stressed bytes");
    result?;
    // Stress is designed to run until interrupted, so a clean Ctrl-C stop is
    // the expected, successful way to terminate it.
    Ok(0)
}

/// Spawn the thread that drains the progress channel, accumulates the total
/// number of bytes processed, and renders the live spinner (bandwidth,
/// generated/verified chunk counts and number of seeds used).
fn spawn_progress(
    rx: mpsc::Receiver<u64>,
    total_bytes: Arc<AtomicU64>,
    scheduler: Arc<Scheduler>,
    no_progress: bool,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let pb = (!no_progress && std::io::stderr().is_terminal()).then(|| {
            let pb = ProgressBar::new_spinner();
            pb.set_draw_target(ProgressDrawTarget::stderr_with_hz(5));
            pb.set_style(ProgressStyle::with_template("{spinner} {msg}").unwrap());
            pb
        });
        let mut last = Instant::now();
        let mut last_bytes = 0u64;
        loop {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(bytes) => {
                    total_bytes.fetch_add(bytes, Ordering::Relaxed);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
            let Some(pb) = &pb else { continue };
            let now = Instant::now();
            let dt = now.duration_since(last);
            if dt >= Duration::from_millis(500) {
                let cur = total_bytes.load(Ordering::Relaxed);
                let bandwidth = (cur - last_bytes) as f64 / dt.as_secs_f64();
                pb.set_message(format!(
                    "{} | generated {} chunks | verified {} chunks | {} seeds",
                    format_bandwidth(bandwidth),
                    scheduler.generated.load(Ordering::Relaxed),
                    scheduler.validated.load(Ordering::Relaxed),
                    scheduler.seeds.load(Ordering::Relaxed),
                ));
                pb.tick();
                last = now;
                last_bytes = cur;
            }
        }
        if let Some(pb) = &pb {
            pb.finish_and_clear();
        }
    })
}

/// Immutable per-run context shared across all of a worker's jobs.
struct WorkerCtx<'a> {
    file: &'a Path,
    /// Stream shape and flags. The `seed` field is unused here; each generate
    /// job uses the seed of the epoch it was claimed in instead.
    stream: StreamParams,
    num_chunks: u64,
    chunks_per_range: u64,
    scheduler: &'a Scheduler,
    tx: &'a mpsc::Sender<u64>,
    cancel: &'a AtomicBool,
}

fn worker(ctx: &WorkerCtx) -> anyhow::Result<()> {
    let mut rng = rand::rng();
    // Allocations reused across every job this worker handles.
    let mut buffer = AVec::<u8, RuntimeAlign>::from_iter(
        DIRECT_ALIGN,
        std::iter::repeat_n(0u8, ctx.stream.io_buffer_size()),
    );
    while !ctx.cancel.load(Ordering::Relaxed) {
        let Some(job) = ctx.scheduler.claim(&mut rng) else {
            // Every range is busy; back off briefly and retry.
            thread::sleep(Duration::from_millis(1));
            continue;
        };
        // A range maps onto write/validate_chunk_range as if it were the slice
        // of a single "thread" whose index is the range number.
        let work = ThreadWork {
            thread_index: job.range,
            chunks_per_thread: ctx.chunks_per_range,
            num_chunks: ctx.num_chunks,
        };
        let start_chunk = job.range * ctx.chunks_per_range;
        let chunks_in_range =
            ((job.range + 1) * ctx.chunks_per_range).min(ctx.num_chunks) - start_chunk;
        let result = match job.action {
            Action::Generate => {
                // Use the seed of the epoch this job was claimed in.
                let params = StreamParams { seed: job.seed, ..ctx.stream.clone() };
                write_chunk_range(
                    ctx.file,
                    &params,
                    &work,
                    buffer.as_mut_slice(),
                    ctx.tx,
                    ctx.cancel,
                )
                .map(|_| ())
            }
            Action::Validate => validate_chunk_range(
                ctx.file,
                ctx.stream.chunk_size,
                job.range,
                ctx.chunks_per_range,
                ctx.num_chunks,
                ctx.stream.stream_size,
                ctx.stream.position,
                ctx.stream.direct,
                // without the padding of the last chunk, only for generation
                &mut buffer[..ctx.stream.io_chunks * ctx.stream.chunk_size],
                ctx.tx,
                ctx.cancel,
            )
            .map(|_| ()),
        };

        let cancelled = ctx.cancel.load(Ordering::Relaxed);
        // On a clean Ctrl-C the chunk-range helpers return early with `Ok` and
        // a partially processed range. Treat that as unsuccessful so a
        // half-written range goes back to `Empty` rather than being marked
        // `Generated` (which would count it and could trigger a seed rotation).
        ctx.scheduler.finish(&job, result.is_ok() && !cancelled);
        if let Err(e) = result {
            // A genuine failure (e.g. a checksum mismatch). Signal every worker
            // to stop so the error is actually reported, instead of the run
            // hanging while `stress` joins peers that would loop forever.
            ctx.cancel.store(true, Ordering::Relaxed);
            return Err(e);
        }
        if cancelled {
            break;
        }
        match job.action {
            Action::Generate => {
                ctx.scheduler.generated.fetch_add(chunks_in_range, Ordering::Relaxed)
            }
            Action::Validate => {
                ctx.scheduler.validated.fetch_add(chunks_in_range, Ordering::Relaxed)
            }
        };
    }
    Ok(())
}

fn resolve_stream_size(args: &StressArgs) -> anyhow::Result<u64> {
    if let Some(size) = args.common.size {
        return direct_aligned_stream_size(size.bytes, size.has_unit, args.common.direct);
    }
    if args.file.exists() {
        let size = read_file_size(&args.file)?;
        if args.position > size {
            return Err(anyhow!(
                "The position {} is greater than the file size {size}",
                args.position
            ));
        }
        return direct_aligned_stream_size(size - args.position, false, args.common.direct);
    }
    Err(anyhow!("Size can't be determined. Use --size to provide a stream size."))
}

/// Format a byte/second rate as a right-aligned, 5-column field with exactly
/// one fractional digit followed by a binary unit, e.g. "  1.2 M/s",
/// " 12.3 M/s", "123.4 M/s". The value is scaled so it renders with at most
/// three integer digits, keeping the spinner column width stable.
fn format_bandwidth(bytes_per_sec: f64) -> String {
    const UNITS: [&str; 6] = ["B", "K", "M", "G", "T", "P"];
    let mut value = bytes_per_sec;
    let mut unit = 0;
    // 999.95, not 1000.0: a value just below 1000 would otherwise be rendered
    // as "1000.0" by the `.1` rounding, overflowing the fixed width.
    while value >= 999.95 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:>5.1} {}/s", UNITS[unit])
}
