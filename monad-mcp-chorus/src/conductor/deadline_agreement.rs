// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use super::{
    ConductorConfig, ConductorError, ConductorOutput, MonadConductor,
    acs::{Acs, AcsOutput},
    types::{NodeId, Slot, Timestamp, WindowId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadlineAgreementMessage<M> {
    pub window: WindowId,
    pub acs_message: M,
}

struct ActiveDeadlineRound<A>
where
    A: Acs<Timestamp>,
{
    target_window: WindowId,
    proposed: bool,
    acs: A,
}

impl<A> ActiveDeadlineRound<A>
where
    A: Acs<Timestamp>,
{
    fn new(target_window: WindowId, context: &A::Context) -> Self {
        let acs = A::new(context);
        Self {
            target_window,
            proposed: false,
            acs,
        }
    }

    // Window whose completion drives this round; None for the genesis round,
    // whose deadline is fixed by config and never proposed on.
    fn opened_window(&self) -> Option<WindowId> {
        self.target_window.checked_prev()
    }

    fn try_propose(
        &mut self,
        config: &ConductorConfig,
        cap: Slot,
        _at: Timestamp,
    ) -> Result<(), ConductorError> {
        let Some(opened_window) = self.opened_window() else {
            return Ok(());
        };

        let sbs_slot = config.sync_boundary_slot(opened_window)?;
        if cap > sbs_slot && !self.proposed {
            // TODO: compute next deadline using at
            let deadline = config.natural_first_slot_deadline(self.target_window)?;
            self.acs.propose(deadline);
            self.proposed = true;
        }
        Ok(())
    }

    fn handle_message(&mut self, sender: NodeId, message: DeadlineAgreementMessage<A::Message>) {
        if message.window != self.target_window {
            tracing::debug!(
                ?sender,
                window = ?message.window,
                target_window = ?self.target_window,
                "ignoring deadline agreement message for inactive window"
            );
            return;
        }

        self.acs.handle_message(sender, message.acs_message);
    }

    fn poll(&mut self) -> Option<DeadlineAgreementMessage<A::Message>> {
        match self.acs.poll()? {
            AcsOutput::Broadcast(message) => Some(DeadlineAgreementMessage {
                window: self.target_window,
                acs_message: message,
            }),
        }
    }

    fn decision(&self) -> Option<Timestamp> {
        // intentionally deferring the next window if local finalization hasn't
        // caught up
        if !self.proposed {
            return None;
        }

        Some(*self.acs.decision()?)
    }
}

pub struct DeadlineAgreementManager<A>
where
    A: Acs<Timestamp>,
{
    // TODO: remove constant context, associate with window
    context: A::Context,
    // Invariant: targets the window at the conductor's open-window cap. The
    // initial round targets `WindowId::FIRST` and is transient
    active_round: ActiveDeadlineRound<A>,
}

impl<A> DeadlineAgreementManager<A>
where
    A: Acs<Timestamp>,
{
    pub fn new(context: A::Context) -> Self {
        let active_round = ActiveDeadlineRound::new(WindowId::FIRST, &context);
        Self {
            context,
            active_round,
        }
    }

    pub fn handle_completion_cap_advance(
        &mut self,
        config: &ConductorConfig,
        cap: Slot,
        at: Timestamp,
    ) -> Result<(), ConductorError> {
        self.active_round.try_propose(config, cap, at)
    }

    pub fn handle_message(
        &mut self,
        sender: NodeId,
        message: DeadlineAgreementMessage<A::Message>,
    ) {
        self.active_round.handle_message(sender, message);
    }

    pub fn poll(&mut self) -> Option<ConductorOutput<MonadConductor<A>>> {
        self.active_round.poll().map(ConductorOutput::Broadcast)
    }

    pub fn decision(&self) -> Option<Timestamp> {
        self.active_round.decision()
    }

    // Starts the round agreeing on the deadline for `target_window`, the
    // window at the conductor's open-window cap.
    pub fn start_round(&mut self, target_window: WindowId) {
        // The inert genesis round never decides; every later round must
        // decide before rotating.
        assert!(
            self.active_round.opened_window().is_none() || self.active_round.decision().is_some(),
            "next round must not start before the active round decides"
        );
        assert_eq!(
            Some(target_window),
            self.active_round.target_window.checked_next(),
            "rounds must advance one window at a time"
        );

        self.active_round = ActiveDeadlineRound::new(target_window, &self.context);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use tracing_test::traced_test;

    use super::{super::types::TimestampDelta, *};

    const GENESIS_DEADLINE: Timestamp = Timestamp::from_millis(100);

    #[derive(Default)]
    struct ImmediateBroadcastAcs {
        outbox: Option<Timestamp>,
        decision: Option<Timestamp>,
    }

    impl Acs<Timestamp> for ImmediateBroadcastAcs {
        type Message = Timestamp;
        type Context = ();

        fn new(_ctx: &Self::Context) -> Self {
            Self::default()
        }

        fn propose(&mut self, deadline: Timestamp) {
            self.outbox = Some(deadline);
            self.decision = Some(deadline);
        }

        fn handle_message(&mut self, _sender: NodeId, message: Self::Message) {
            self.decision = Some(message);
        }

        fn decision(&self) -> Option<&Timestamp> {
            self.decision.as_ref()
        }

        fn poll(&mut self) -> Option<AcsOutput<Self::Message>> {
            self.outbox.take().map(AcsOutput::Broadcast)
        }
    }

    fn nz(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    fn config() -> ConductorConfig {
        ConductorConfig::new(
            nz(10),
            nz(8),
            TimestampDelta::from_nanos(10),
            GENESIS_DEADLINE,
        )
        .unwrap()
    }

    fn genesis_plus(nanos: u64) -> Timestamp {
        GENESIS_DEADLINE
            .checked_add_delta(TimestampDelta::from_nanos(nanos))
            .unwrap()
    }

    #[test]
    fn natural_deadline_is_anchored_to_genesis() {
        assert_eq!(
            config()
                .natural_first_slot_deadline(WindowId::FIRST)
                .unwrap(),
            GENESIS_DEADLINE
        );
        assert_eq!(
            config().natural_first_slot_deadline(WindowId(1)).unwrap(),
            genesis_plus(100)
        );
    }

    #[test]
    fn natural_deadline_detects_overflow() {
        let config = ConductorConfig::new(
            nz(2),
            nz(1),
            TimestampDelta::from_nanos(u64::MAX),
            GENESIS_DEADLINE,
        )
        .unwrap();
        assert_eq!(
            config.natural_first_slot_deadline(WindowId(1)),
            Err(ConductorError::ArithmeticOverflow)
        );
    }

    #[test]
    fn manager_routes_messages_and_completes_successive_rounds() {
        let config = config();
        let mut manager = DeadlineAgreementManager::<ImmediateBroadcastAcs>::new(());
        manager.start_round(WindowId(1));

        assert_eq!(manager.decision(), None);
        manager
            .handle_completion_cap_advance(&config, Slot(8), Timestamp::from_nanos(1_000))
            .unwrap();
        match manager.poll() {
            Some(ConductorOutput::Broadcast(message)) => {
                assert_eq!(
                    message,
                    DeadlineAgreementMessage {
                        window: WindowId(1),
                        acs_message: genesis_plus(100),
                    }
                );
            }
            _ => panic!("expected conductor broadcast"),
        }
        assert!(manager.poll().is_none());
        assert_eq!(manager.decision(), Some(genesis_plus(100)));

        manager.start_round(WindowId(2));
        assert_eq!(manager.decision(), None);
        manager
            .handle_completion_cap_advance(&config, Slot(18), Timestamp::from_nanos(2_000))
            .unwrap();
        match manager.poll() {
            Some(ConductorOutput::Broadcast(message)) => {
                assert_eq!(
                    message,
                    DeadlineAgreementMessage {
                        window: WindowId(2),
                        acs_message: genesis_plus(200),
                    }
                );
            }
            _ => panic!("expected conductor broadcast"),
        }
        assert_eq!(manager.decision(), Some(genesis_plus(200)));
    }

    #[test]
    #[traced_test]
    fn messages_for_inactive_windows_are_ignored() {
        let config = config();
        let mut manager = DeadlineAgreementManager::<ImmediateBroadcastAcs>::new(());
        manager.start_round(WindowId(1));

        manager
            .handle_completion_cap_advance(&config, Slot(8), Timestamp::from_nanos(1_000))
            .unwrap();
        assert_eq!(manager.decision(), Some(genesis_plus(100)));

        // a future-window message must not reach the ACS: a leak would
        // overwrite the decision with 220
        manager.handle_message(
            NodeId::dummy(1),
            DeadlineAgreementMessage {
                window: WindowId(2),
                acs_message: Timestamp::from_nanos(220),
            },
        );
        assert!(logs_contain(
            "ignoring deadline agreement message for inactive window"
        ));
        assert_eq!(manager.decision(), Some(genesis_plus(100)));

        // an active-window message is delivered and overrides the decision
        let round_message = DeadlineAgreementMessage {
            window: WindowId(1),
            acs_message: Timestamp::from_nanos(110),
        };
        manager.handle_message(NodeId::dummy(1), round_message.clone());
        assert_eq!(manager.decision(), Some(Timestamp::from_nanos(110)));

        // the same message targets a stale window once the round advances
        manager.start_round(WindowId(2));
        manager
            .handle_completion_cap_advance(&config, Slot(18), Timestamp::from_nanos(2_000))
            .unwrap();
        manager.handle_message(NodeId::dummy(1), round_message);
        assert_eq!(manager.decision(), Some(genesis_plus(200)));
    }
}
