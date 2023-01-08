use std::{convert::Infallible, fmt::Display, sync::Arc};

use okaywal::{Configuration, LogVoid, WriteAheadLog};
use tempfile::TempDir;
use timings::{Benchmark, BenchmarkImplementation, Label, LabeledTimings, Timings};

fn main() {
    let measurements = Timings::default();
    let bench = Benchmark::for_each_config(vec![
        InsertConfig {
            number_of_bytes: 256,
            iters: 500,
        },
        InsertConfig {
            number_of_bytes: 1024,
            iters: 250,
        },
        InsertConfig {
            number_of_bytes: 4096,
            iters: 125,
        },
        InsertConfig {
            number_of_bytes: 1024 * 1024,
            iters: 75,
        },
    ])
    .with_each_number_of_threads([1, 2, 4, 8, 16])
    .with::<OkayWal>();

    #[cfg(feature = "sharded-log")]
    let bench = bench.with::<shardedlog::ShardedLog>();

    #[cfg(feature = "postgres")]
    let bench = bench.with::<postgres::Postgres>();

    #[cfg(feature = "sqlite")]
    let bench = bench.with::<sqlite::SQLite>();

    bench.run(&measurements).unwrap();

    let stats = measurements.wait_for_stats();
    timings::print_table_summaries(&stats).unwrap();
}

#[derive(Copy, Clone, Debug)]
struct InsertConfig {
    number_of_bytes: usize,
    iters: usize,
}

struct OkayWal {
    config: InsertConfig,
    _dir: Arc<TempDir>,
    log: WriteAheadLog,
}

impl BenchmarkImplementation<Label, InsertConfig, Infallible> for OkayWal {
    type SharedConfig = (InsertConfig, Arc<TempDir>, WriteAheadLog);

    fn label(number_of_threads: usize, _config: &InsertConfig) -> Label {
        Label::from(format!("okaywal-{:02}t", number_of_threads))
    }

    fn initialize_shared_config(
        _number_of_threads: usize,
        config: &InsertConfig,
    ) -> Result<Self::SharedConfig, Infallible> {
        let dir = Arc::new(TempDir::new_in(".").unwrap());
        let log = Configuration::default_for(&*dir).open(LogVoid).unwrap();
        Ok((*config, dir, log))
    }

    fn reset(_shutting_down: bool) -> Result<(), Infallible> {
        Ok(())
    }

    fn initialize(
        _number_of_threads: usize,
        (config, dir, log): Self::SharedConfig,
    ) -> Result<Self, Infallible> {
        Ok(Self {
            config,
            log,
            _dir: dir,
        })
    }

    fn measure(&mut self, measurements: &LabeledTimings<Label>) -> Result<(), Infallible> {
        let metric = Label::from(format!("commit-{}", Bytes(self.config.number_of_bytes)));
        let data = vec![42; self.config.number_of_bytes];
        for _ in 0..self.config.iters {
            let measurement = measurements.begin(metric.clone());
            let mut session = self.log.begin_entry().unwrap();
            session.write_chunk(&data).unwrap();
            session.commit().unwrap();
            measurement.finish();
        }
        Ok(())
    }
}

#[cfg(feature = "sharded-log")]
mod shardedlog {
    use super::*;

    pub struct ShardedLog {
        config: InsertConfig,
        _dir: Arc<TempDir>,
        log: sharded_log::ShardedLog,
    }

    impl BenchmarkImplementation<Label, InsertConfig, Infallible> for ShardedLog {
        type SharedConfig = (InsertConfig, Arc<TempDir>, sharded_log::ShardedLog);

        fn label(number_of_threads: usize, _config: &InsertConfig) -> Label {
            Label::from(format!("sharded-log-{:02}t", number_of_threads))
        }

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
            _number_of_threads: usize,
            (config, dir, log): Self::SharedConfig,
        ) -> Result<Self, Infallible> {
            Ok(Self {
                config,
                log,
                _dir: dir,
            })
        }

        fn measure(&mut self, measurements: &LabeledTimings<Label>) -> Result<(), Infallible> {
            let metric = Label::from(format!("commit-{}", Bytes(self.config.number_of_bytes)));
            let data = vec![42; self.config.number_of_bytes];
            for _ in 0..self.config.iters {
                let measurement = measurements.begin(metric.clone());
                self.log.write_batch(&[&data]).unwrap();
                self.log.flush().unwrap();
                measurement.finish();
            }
            Ok(())
        }
    }
}

#[cfg(feature = "postgres")]
mod postgres {
    use ::postgres::NoTls;

    use super::*;

    #[derive(Clone, Debug)]
    pub struct Postgres {
        config: InsertConfig,
        pg_config: ::postgres::Config,
    }

    impl BenchmarkImplementation<Label, InsertConfig, Infallible> for Postgres {
        type SharedConfig = Self;

        fn label(number_of_threads: usize, _config: &InsertConfig) -> Label {
            Label::from(format!("postgres-{:02}t", number_of_threads))
        }

        fn initialize_shared_config(
            _number_of_threads: usize,
            config: &InsertConfig,
        ) -> Result<Self::SharedConfig, Infallible> {
            let mut pg_config = ::postgres::Config::new();
            pg_config
                .dbname("bench")
                .host("localhost")
                .user("bencher")
                .password("password");
            let mut client = pg_config.connect(NoTls).unwrap();
            client
                .execute("DROP TABLE IF EXISTS okaywal_inserts;", &[])
                .unwrap();
            client
                .execute("CREATE TABLE okaywal_inserts(data bytea);", &[])
                .unwrap();
            Ok(Self {
                config: *config,
                pg_config,
            })
        }

        fn reset(_shutting_down: bool) -> Result<(), Infallible> {
            Ok(())
        }

        fn initialize(
            _number_of_threads: usize,
            config: Self::SharedConfig,
        ) -> Result<Self, Infallible> {
            Ok(config)
        }

        fn measure(&mut self, measurements: &LabeledTimings<Label>) -> Result<(), Infallible> {
            let mut client = self.pg_config.connect(NoTls).unwrap();
            let metric = Label::from(format!("commit-{}", Bytes(self.config.number_of_bytes)));
            let data = vec![42_u8; self.config.number_of_bytes];
            for _ in 0..self.config.iters {
                let measurement = measurements.begin(metric.clone());
                client
                    .execute("INSERT INTO okaywal_inserts (data) values ($1)", &[&data])
                    .unwrap();

                measurement.finish();
            }
            Ok(())
        }
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {

    use std::sync::Mutex;

    use rusqlite::Connection;
    use tempfile::NamedTempFile;

    use super::*;

    #[derive(Clone, Debug)]
    pub struct SQLite {
        config: InsertConfig,
        sqlite: Arc<Mutex<Connection>>,
        _file: Arc<NamedTempFile>,
    }

    impl BenchmarkImplementation<Label, InsertConfig, Infallible> for SQLite {
        type SharedConfig = Self;

        fn label(number_of_threads: usize, _config: &InsertConfig) -> Label {
            Label::from(format!("sqlite-{:02}t", number_of_threads))
        }

        fn initialize_shared_config(
            _number_of_threads: usize,
            config: &InsertConfig,
        ) -> Result<Self::SharedConfig, Infallible> {
            let tmp_file = NamedTempFile::new_in(".").unwrap();
            let sqlite = Connection::open(&tmp_file).unwrap();
            sqlite
                .busy_timeout(std::time::Duration::from_secs(3600))
                .unwrap();

            #[cfg(any(target_os = "macos", target_os = "ios"))]
            {
                // On macOS with built-in SQLite versions, despite the name and the SQLite
                // documentation, this pragma makes SQLite use `fcntl(_, F_BARRIER_FSYNC,
                // _)`. There's not a good practical way to make rusqlite's access of SQLite
                // on macOS to use `F_FULLFSYNC`, which skews benchmarks heavily in favor of
                // SQLite when not enabling this feature.
                //
                // Enabling this feature reduces the durability guarantees, which breaks
                // ACID compliance. Unless performance is critical on macOS or you know that
                // ACID compliance is not important for your application, this feature
                // should be left disabled.
                //
                // <https://bonsaidb.io/blog/acid-on-apple/>
                // <https://www.sqlite.org/pragma.html#pragma_fullfsync>
                sqlite.pragma_update(None, "fullfsync", "on").unwrap();
                println!("The shipping version of SQLite on macOS is not actually ACID-compliant. See this blog post:\n<https://bonsaidb.io/blog/acid-on-apple/>");
            }
            sqlite.pragma_update(None, "journal_mode", "WAL").unwrap();

            sqlite
                .execute("create table if not exists okaywal_inserts (data BLOB)", [])
                .unwrap();
            Ok(Self {
                config: *config,
                sqlite: Arc::new(Mutex::new(sqlite)),
                _file: Arc::new(tmp_file),
            })
        }

        fn reset(_shutting_down: bool) -> Result<(), Infallible> {
            Ok(())
        }

        fn initialize(
            _number_of_threads: usize,
            config: Self::SharedConfig,
        ) -> Result<Self, Infallible> {
            Ok(config)
        }

        fn measure(&mut self, measurements: &LabeledTimings<Label>) -> Result<(), Infallible> {
            let metric = Label::from(format!("commit-{}", Bytes(self.config.number_of_bytes)));
            let data = vec![42_u8; self.config.number_of_bytes];
            for _ in 0..self.config.iters {
                let measurement = measurements.begin(metric.clone());
                let client = self.sqlite.lock().unwrap(); // SQLite doesn't have a way to allow multi-threaded write access to a single database.
                client
                    .execute("INSERT INTO okaywal_inserts (data) values ($1)", [&data])
                    .unwrap();
                drop(client);
                measurement.finish();
            }
            Ok(())
        }
    }
}

struct Bytes(usize);

impl Display for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (size_number, size_label) = match self.0 {
            0..=1023 => (self.0, "B"),
            1_024..=1048575 => (self.0 / 1024, "KB"),
            1048576..=1073741823 => (self.0 / 1024 / 1024, "MB"),
            _ => unreachable!(),
        };
        write!(f, "{}{}", size_number, size_label)
    }
}
