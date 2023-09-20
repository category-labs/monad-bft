#[cfg(test)]
mod test {
    use std::{array::TryFromSliceError, fs::OpenOptions};

    use monad_executor::{Identifiable, Message, State};
    use monad_testutil::block::MockBlock;
    use monad_types::{Deserializable, Serializable};
    use monad_wal::{
        wal::{WALogger, WALoggerConfig},
        PersistenceLogger,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestEvent {
        data: i32,
    }

    impl Serializable<Vec<u8>> for TestEvent {
        fn serialize(&self) -> Vec<u8> {
            self.data.to_be_bytes().to_vec()
        }
    }

    impl Deserializable<[u8]> for TestEvent {
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
    }

    impl PartialEq for VecState {
        fn eq(&self, other: &Self) -> bool {
            self.events.eq(&other.events)
        }
    }

    impl Eq for VecState {}

    struct VecStateConfig {}

    #[derive(Clone)]
    struct MockMessage;

    impl Identifiable for MockMessage {
        type Id = i32;

        fn id(&self) -> Self::Id {
            0
        }
    }

    impl Message for MockMessage {
        type Event = TestEvent;

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
        type Block = MockBlock;
        type Checkpoint = ();

        fn init(
            _config: Self::Config,
        ) -> (
            Self,
            Vec<
                monad_executor::Command<
                    Self::Message,
                    Self::OutboundMessage,
                    Self::Block,
                    Self::Checkpoint,
                >,
            >,
        ) {
            let state = VecState { events: Vec::new() };
            (state, Vec::new())
        }

        fn update(
            &mut self,
            event: Self::Event,
        ) -> Vec<
            monad_executor::Command<
                Self::Message,
                Self::OutboundMessage,
                Self::Block,
                Self::Checkpoint,
            >,
        > {
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
        use std::{fs, fs::create_dir_all};

        use tempfile::tempdir;

        let input1 = generate_test_events(10);
        let input1_len = input1.len();
        let input2 = generate_test_events(7);

        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log1_path = tmpdir.path().join("wal1");
        let logger1_config = WALoggerConfig {
            file_path: log1_path.clone(),
            sync: false,
        };

        let (mut logger1, events1): (WALogger<TestEvent>, _) =
            WALogger::new(logger1_config).unwrap();
        assert!(events1.is_empty());
        let (mut state1, _) = VecState::init(VecStateConfig {});
        for event in events1 {
            state1.update(event);
        }

        // driver loop (simulate executor by iterating events)
        for (i, e) in input1.into_iter().enumerate() {
            logger1.push(&e).unwrap();
            // simulate node failure when appending event by truncating the file
            // state is not updated
            if i == input1_len - 1 {
                let file_len = fs::metadata(&log1_path).unwrap().len();
                let payload_len = Serializable::<Vec<u8>>::serialize(&e).len() as u64;

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
        let logger2_config = WALoggerConfig {
            file_path: log2_path.clone(),
            sync: false,
        };

        let (mut logger2, events2) = WALogger::new(logger2_config).unwrap();
        assert!(!events2.is_empty());
        let (mut state2, _) = VecState::init(VecStateConfig {});
        for event in events2 {
            state2.update(event);
        }
        assert_eq!(state1, state2);

        // another driver loop
        for e in input2.into_iter() {
            logger2.push(&e).unwrap();
            state2.update(e);
        }

        // init 3rd state from wal (to test the wal is in the right state even if it had incomplete messages)
        let log2_len = fs::metadata(&log2_path).unwrap().len();
        let log3_path = tmpdir.path().join("wal3");
        let copied = fs::copy(&log2_path, &log3_path).unwrap();
        assert_eq!(log2_len, copied);
        let logger3_config = WALoggerConfig {
            file_path: log3_path,
            sync: false,
        };

        let (_, events3) = WALogger::new(logger3_config).unwrap();
        assert!(!events3.is_empty());
        let (mut state3, _) = VecState::init(VecStateConfig {});
        for event in events3 {
            state3.update(event);
        }
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
