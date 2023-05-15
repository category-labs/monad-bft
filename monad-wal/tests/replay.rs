#[cfg(test)]
mod test {
    use std::{array::TryFromSliceError, fs::OpenOptions, path::PathBuf};

    use monad_executor::{Message, State};
    use monad_types::{Deserializable, Serializable};
    use monad_wal::wal::WALogger;
    use monad_wal::PersistenceLogger;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestEvent {
        data: i32,
    }

    impl Serializable for TestEvent {
        fn serialize(&self) -> Vec<u8> {
            self.data.to_be_bytes().to_vec()
        }
    }

    impl Deserializable for TestEvent {
        type ReadError = TryFromSliceError;

        fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
            let buf: [u8; 4] = message.try_into()?;
            Ok(Self {
                data: i32::from_be_bytes(buf),
            })
        }
    }

    #[derive(Debug)]
    struct VecState {
        events: Vec<TestEvent>,
        wal: WALogger<TestEvent>,
    }

    impl VecState {
        fn persist_event(&mut self, event: &TestEvent) {
            self.wal.push(event).unwrap()
        }
    }

    impl PartialEq for VecState {
        fn eq(&self, other: &Self) -> bool {
            self.events.eq(&other.events)
        }
    }

    impl Eq for VecState {}

    struct VecStateConfig {
        wal_path: PathBuf,
    }

    #[derive(Clone)]
    struct MockMessage;

    impl Message for MockMessage {
        type Event = TestEvent;

        type Id = i32;

        fn id(&self) -> Self::Id {
            0
        }

        fn event(self, _from: monad_executor::PeerId) -> Self::Event {
            TestEvent { data: 0 }
        }
    }

    impl AsRef<MockMessage> for MockMessage {
        fn as_ref(&self) -> &MockMessage {
            self
        }
    }

    impl State for VecState {
        type Config = VecStateConfig;
        type Event = TestEvent;
        type OutboundMessage = MockMessage;
        type Message = MockMessage;

        fn init(
            config: Self::Config,
        ) -> (
            Self,
            Vec<monad_executor::Command<Self::Message, Self::OutboundMessage>>,
        ) {
            let (wal, events): (WALogger<_>, Vec<TestEvent>) =
                WALogger::new(config.wal_path).unwrap();
            let mut state = VecState {
                events: Vec::new(),
                wal,
            };
            for e in events {
                state.update(e);
            }
            (state, Vec::new())
        }

        fn update(
            &mut self,
            event: Self::Event,
        ) -> Vec<monad_executor::Command<Self::Message, Self::OutboundMessage>> {
            self.events.push(event);
            Vec::new()
        }
    }

    fn generate_test_events(num: i32) -> Vec<TestEvent> {
        (0..num).map(|i| TestEvent { data: i }).collect()
    }

    enum TestConfig {
        Full,
        IncompletePayload,
        IncompleteHeader,
    }

    fn test_replay_configurable(test_config: TestConfig) {
        // setup
        use std::fs;
        use std::fs::create_dir_all;
        use tempfile::tempdir;

        let events1 = generate_test_events(10);
        let events1_len = events1.len();
        let events2 = generate_test_events(7);

        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log1_path = tmpdir.path().join("wal1");
        let config1 = VecStateConfig {
            wal_path: log1_path.clone(),
        };

        let (mut state1, _) = VecState::init(config1);

        // driver loop (simulate executor by iterating events)
        for (i, e) in events1.into_iter().enumerate() {
            state1.persist_event(&e);
            // simulate node failure when appending event by truncating the file
            // state is not updated
            if i == events1_len - 1 {
                let file_len = fs::metadata(&log1_path).unwrap().len();
                let payload_len = e.serialize().len() as u64;

                let truncated_len = match test_config {
                    TestConfig::Full => file_len,
                    TestConfig::IncompletePayload => file_len - 1,
                    TestConfig::IncompleteHeader => file_len - payload_len - 1,
                };

                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&log1_path)
                    .unwrap();
                file.set_len(truncated_len).unwrap();

                if truncated_len < file_len {
                    break;
                }
            }

            state1.update(e);
        }

        // init another state from the wal, assert equal
        let log1_len = fs::metadata(&log1_path).unwrap().len();
        let log2_path = tmpdir.path().join("wal2");
        let copied = fs::copy(&log1_path, &log2_path).unwrap();
        assert_eq!(log1_len, copied);

        let config2 = VecStateConfig {
            wal_path: log2_path.clone(),
        };

        let (mut state2, _) = VecState::init(config2);
        assert_eq!(state1, state2);

        // another driver loop
        for e in events2.into_iter() {
            state2.persist_event(&e);
            state2.update(e);
        }

        // init 3rd state from wal (to test the wal is in the right state even if it had incomplete messages)
        let log2_len = fs::metadata(&log2_path).unwrap().len();
        let log3_path = tmpdir.path().join("wal3");
        let copied = fs::copy(&log2_path, &log3_path).unwrap();
        assert_eq!(log2_len, copied);

        let config3 = VecStateConfig {
            wal_path: log3_path,
        };
        let (state3, _) = VecState::init(config3);
        assert_eq!(state2, state3);
    }

    #[test]
    fn test_replay() {
        test_replay_configurable(TestConfig::Full);
    }

    #[test]
    fn test_replay_incomplete_payload() {
        test_replay_configurable(TestConfig::IncompletePayload);
    }

    #[test]
    fn test_replay_incomplete_header() {
        test_replay_configurable(TestConfig::IncompleteHeader);
    }
}
