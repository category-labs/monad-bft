use std::env;

use log::LevelFilter;
use log4rs::{
    append::{
        console::{ConsoleAppender, Target},
        rolling_file::policy::compound::{
            roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
        },
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
    filter::threshold::ThresholdFilter,
};

pub struct Logger {
    // Maximum file size for a which a new file should then be created
    // (e.g. 1024 is 1KB)
    trigger_file_size: u64,
    // Number of files in log rotation
    log_file_count: u32,
    // Location where logs will be written to
    file_path: String,
    // Location of rotated logs
    archive_pattern: String,
}

impl Logger {
    pub fn new(
        trigger_file_size: u64,
        log_file_count: u32,
        file_path: String,
        archive_pattern: String,
    ) -> Self {
        Logger {
            trigger_file_size,
            log_file_count,
            file_path,
            archive_pattern,
        }
    }

    pub fn init(self) {
        // Set log level
        let rust_log_level = env::var("RUST_LOG").unwrap_or_default();
        let level_filter = match rust_log_level.to_lowercase().as_str() {
            "trace" => LevelFilter::Trace,
            "debug" => LevelFilter::Debug,
            "info" => LevelFilter::Info,
            "warn" => LevelFilter::Warn,
            "error" => LevelFilter::Error,
            _ => LevelFilter::Info, // default to info
        };

        // Create file logging policy
        let stderr = ConsoleAppender::builder().target(Target::Stderr).build();
        let trigger = SizeTrigger::new(self.trigger_file_size);
        let roller = FixedWindowRoller::builder()
            .build(self.archive_pattern.as_str(), self.log_file_count)
            .unwrap();
        let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

        // Logging to file with rotation
        let logfile = log4rs::append::rolling_file::RollingFileAppender::builder()
            .encoder(Box::new(PatternEncoder::new(
                "{d(%Y-%m-%dT%H:%M:%SZ)} {l} - {m}\n",
            )))
            .build(self.file_path.as_str(), Box::new(policy))
            .unwrap();

        let config = Config::builder()
            .appender(Appender::builder().build("logfile", Box::new(logfile)))
            .appender(
                Appender::builder()
                    .filter(Box::new(ThresholdFilter::new(level_filter)))
                    .build("stderr", Box::new(stderr)),
            )
            .build(
                Root::builder()
                    .appender("logfile")
                    .appender("stderr")
                    .build(level_filter),
            )
            .unwrap();

        let _handle = log4rs::init_config(config);
    }
}
