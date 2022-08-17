use std::{convert::Infallible, sync::Arc};

use okaywal::{Config, VoidArchiver, WriteAheadLog};
use tempfile::TempDir;
use timings::{Benchmark, BenchmarkImplementation, Label, Timings};

fn main() {
    let (measurements, stats) = Timings::new();
    Benchmark::for_each_config(vec![
        InsertConfig {
            number_of_bytes: 256,
            iters: 1000,
        },
        InsertConfig {
            number_of_bytes: 1024,
            iters: 500,
        },
        InsertConfig {
            number_of_bytes: 4096,
            iters: 250,
        },
        InsertConfig {
            number_of_bytes: 1024 * 1024,
            iters: 100,
        },
    ])
    .with_each_number_of_threads([1, 2, 4, 8, 16])
    .with::<OkayWal>()
    .with::<ShardedLog>()
    .run(&measurements)
    .unwrap();

    drop(measurements);

    let stats = stats.join().unwrap();
    timings::print_table_summaries(&stats).unwrap();
}

#[derive(Copy, Clone, Debug)]
struct InsertConfig {
    number_of_bytes: usize,
    iters: usize,
}

struct OkayWal {
    number_of_threads: usize,
    config: InsertConfig,
    _dir: Arc<TempDir>,
    log: WriteAheadLog,
}

impl BenchmarkImplementation<Label, InsertConfig, Infallible> for OkayWal {
    type SharedConfig = (InsertConfig, Arc<TempDir>, WriteAheadLog);

    fn initialize_shared_config(
        number_of_threads: usize,
        config: &InsertConfig,
    ) -> Result<Self::SharedConfig, Infallible> {
        let dir = Arc::new(TempDir::new_in(".").unwrap());
        let log = WriteAheadLog::recover_with_config(
            dir.as_ref(),
            VoidArchiver,
            Config {
                active_segment_limit: number_of_threads,
                ..Config::default()
            },
        )
        .unwrap();
        Ok((*config, dir, log))
    }

    fn reset(_shutting_down: bool) -> Result<(), Infallible> {
        Ok(())
    }

    fn initialize(
        number_of_threads: usize,
        (config, dir, log): Self::SharedConfig,
    ) -> Result<Self, Infallible> {
        Ok(Self {
            config,
            number_of_threads,
            log,
            _dir: dir,
        })
    }

    fn measure(&mut self, measurements: &timings::Timings<Label>) -> Result<(), Infallible> {
        let label = Label::from(format!("okaywal-{:02}t", self.number_of_threads));
        let (size_number, size_label) = match self.config.number_of_bytes {
            0..=1023 => (self.config.number_of_bytes, "B"),
            1_024..=1048575 => (self.config.number_of_bytes / 1024, "KB"),
            1048576..=1073741823 => (self.config.number_of_bytes / 1024 / 1024, "MB"),
            _ => unreachable!(),
        };
        let metric = Label::from(format!("commit-{size_number}{size_label}"));
        let data = vec![42; self.config.number_of_bytes];
        for _ in 0..self.config.iters {
            let measurement = measurements.begin(label.clone(), metric.clone());
            let mut session = self.log.write().unwrap();
            session.write_all(&data).unwrap();
            session.commit().unwrap();
            measurement.finish();
        }
        Ok(())
    }
}

struct ShardedLog {
    number_of_threads: usize,
    config: InsertConfig,
    _dir: Arc<TempDir>,
    log: sharded_log::ShardedLog,
}

impl BenchmarkImplementation<Label, InsertConfig, Infallible> for ShardedLog {
    type SharedConfig = (InsertConfig, Arc<TempDir>, sharded_log::ShardedLog);

    fn initialize_shared_config(
        number_of_threads: usize,
        config: &InsertConfig,
    ) -> Result<Self::SharedConfig, Infallible> {
        let dir = Arc::new(TempDir::new_in(".").unwrap());
        let log = sharded_log::Config {
            path: dir.path().to_path_buf(),
            shards: u8::try_from(number_of_threads).unwrap(),
            ..sharded_log::Config::default()
        }
        .create()
        .unwrap();
        Ok((*config, dir, log))
    }

    fn reset(_shutting_down: bool) -> Result<(), Infallible> {
        Ok(())
    }

    fn initialize(
        number_of_threads: usize,
        (config, dir, log): Self::SharedConfig,
    ) -> Result<Self, Infallible> {
        Ok(Self {
            config,
            number_of_threads,
            log,
            _dir: dir,
        })
    }

    fn measure(&mut self, measurements: &timings::Timings<Label>) -> Result<(), Infallible> {
        let label = Label::from(format!("sharded-log-{:02}t", self.number_of_threads));
        let (size_number, size_label) = match self.config.number_of_bytes {
            0..=1023 => (self.config.number_of_bytes, "B"),
            1_024..=1048575 => (self.config.number_of_bytes / 1024, "KB"),
            1048576..=1073741823 => (self.config.number_of_bytes / 1024 / 1024, "MB"),
            _ => unreachable!(),
        };
        let metric = Label::from(format!("commit-{size_number}{size_label}"));
        let data = vec![42; self.config.number_of_bytes];
        for _ in 0..self.config.iters {
            let measurement = measurements.begin(label.clone(), metric.clone());
            self.log.write_batch(&[&data]).unwrap();
            self.log.flush().unwrap();
            measurement.finish();
        }
        Ok(())
    }
}
