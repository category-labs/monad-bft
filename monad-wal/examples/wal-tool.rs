#![allow(deprecated)]
use std::{
    cmp::min,
    collections::{BTreeMap, HashMap},
    fs,
    io::{self, stdout, BufWriter, Result, Stdout},
    path::PathBuf,
    rc::Rc,
};

use chrono::{DateTime, TimeDelta, Utc};
use clap::{ArgGroup, Args as ClapArgs, Parser, Subcommand};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use itertools::Itertools;
use monad_bls::BlsSignatureCollection;
use monad_consensus::messages::consensus_message::ProtocolMessage;
use monad_consensus_types::block::BlockType;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_executor_glue::{LogFriendlyMonadEvent, MonadEvent};
use monad_secp::SecpSignature;
use monad_types::Epoch;
use monad_wal::wal::WALoggerConfig;
use ratatui::{
    prelude::*,
    widgets::{block::*, *},
    Frame,
};
use serde::Serialize;

type SigType = SecpSignature;
type SigColType = BlsSignatureCollection<CertificateSignaturePubKey<SigType>>;
type WalEvent = MonadEvent<SigType, SigColType>;
type WrappedEvent = LogFriendlyMonadEvent<SigType, SigColType>;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    wal_path: PathBuf,

    #[arg(long, default_value_t = 0)]
    start: usize,

    #[arg(long, default_value_t = usize::MAX)]
    end: usize,

    #[command(subcommand)]
    mode: ModeCommand,
}

#[derive(Debug, ClapArgs)]
#[clap(group(ArgGroup::new("method").required(true).args(&["stdout", "file"])))]
struct Output {
    #[arg(long, value_name = "STDOUT")]
    stdout: bool,
    #[arg(long, value_name = "FILE")]
    file: Option<PathBuf>,
}

#[derive(Debug, Default, Subcommand)]
enum ModeCommand {
    #[default]
    View,
    Stat(Output),
}

type Tui = Terminal<CrosstermBackend<Stdout>>;

fn tui_restore() -> std::io::Result<()> {
    stdout().execute(LeaveAlternateScreen)?;
    disable_raw_mode()?;

    Ok(())
}

pub fn tui_init() -> std::io::Result<Tui> {
    stdout().execute(EnterAlternateScreen)?;
    enable_raw_mode()?;
    Terminal::new(CrosstermBackend::new(stdout()))
}

enum DeltaWidgetState {
    AsyncStateDelta,
    ConsensusDelta,
    AsyncStateChart,
    ConsensusChart,
}

pub struct MainView {
    count_widget: EventCountsWidget,
    async_delta_widget: EventDeltaWidget,
    consensus_delta_widget: EventDeltaWidget,
    event_list_widget: EventListWidget,

    async_delta_chart: EventDeltaChart,
    consensus_delta_chart: EventDeltaChart,

    delta_widget_state: DeltaWidgetState,
    exit: bool,
}

impl MainView {
    pub fn new(wal_path: PathBuf, start: usize, end: usize) -> Self {
        let logger_config: WALoggerConfig<WrappedEvent> = WALoggerConfig::new(wal_path, true);

        let wal_events = logger_config
            .events()
            .skip(start)
            .take(end.checked_sub(start).expect("end < start"))
            .collect_vec();

        let rc_wal_events = Rc::new(wal_events);
        let count_widget = EventCountsWidget::new(rc_wal_events.clone());
        let async_delta_widget =
            EventDeltaWidget::new("Async State Event", rc_wal_events.clone(), |event| {
                matches!(event, MonadEvent::AsyncStateVerifyEvent(_))
            });
        let consensus_delta_widget =
            EventDeltaWidget::new("Consensus Event", rc_wal_events.clone(), |event| {
                matches!(event, MonadEvent::ConsensusEvent(_))
            });
        let async_delta_chart =
            EventDeltaChart::new("Async State Event", rc_wal_events.clone(), |event| {
                matches!(event, MonadEvent::AsyncStateVerifyEvent(_))
            });
        let consensus_delta_chart =
            EventDeltaChart::new("Consensus State Event", rc_wal_events.clone(), |event| {
                matches!(event, MonadEvent::ConsensusEvent(_))
            });
        let event_list_widget = EventListWidget::new(rc_wal_events);

        Self {
            count_widget,
            async_delta_widget,
            consensus_delta_widget,
            event_list_widget,
            async_delta_chart,
            consensus_delta_chart,
            delta_widget_state: DeltaWidgetState::AsyncStateDelta,
            exit: false,
        }
    }

    /// runs the application's main loop until the user quits
    pub fn run(&mut self, terminal: &mut Tui) -> io::Result<()> {
        while !self.exit {
            terminal.draw(|frame| self.render_frame(frame))?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn render_frame(&self, frame: &mut Frame) {
        let layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(frame.size());

        let left_panel = Layout::default()
            .direction(Direction::Vertical)
            .constraints(vec![Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(layout[0]);

        match self.delta_widget_state {
            DeltaWidgetState::AsyncStateDelta => {
                frame.render_widget(&self.async_delta_widget, left_panel[0])
            }
            DeltaWidgetState::ConsensusDelta => {
                frame.render_widget(&self.consensus_delta_widget, left_panel[0])
            }
            DeltaWidgetState::AsyncStateChart => {
                frame.render_widget(&self.async_delta_chart, left_panel[0])
            }
            DeltaWidgetState::ConsensusChart => {
                frame.render_widget(&self.consensus_delta_chart, left_panel[0])
            }
        }

        frame.render_widget(&self.count_widget, left_panel[1]);
        frame.render_widget(&self.event_list_widget, layout[1]);
    }

    fn handle_events(&mut self) -> io::Result<()> {
        match event::read()? {
            // it's important to check that the event is a key press event as
            // crossterm also emits key release and repeat events on Windows.
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                self.handle_key_event(key_event)
            }
            _ => {}
        };
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        let modifiers = key_event.modifiers;
        match key_event.code {
            KeyCode::Char('q') => self.exit(),
            KeyCode::Char('r') => self.count_widget.load_counts(),
            KeyCode::Char('1') => self.delta_widget_state = DeltaWidgetState::AsyncStateDelta,
            KeyCode::Char('2') => self.delta_widget_state = DeltaWidgetState::ConsensusDelta,
            KeyCode::Char('3') => self.delta_widget_state = DeltaWidgetState::AsyncStateChart,
            KeyCode::Char('4') => self.delta_widget_state = DeltaWidgetState::ConsensusChart,
            KeyCode::Char('s') => {
                if self.async_delta_widget.start_index > 0 {
                    self.async_delta_widget.start_index -= 1;
                }
                if self.consensus_delta_widget.start_index > 0 {
                    self.consensus_delta_widget.start_index -= 1;
                }
            }
            KeyCode::Char('f') => {
                if self.async_delta_widget.start_index < self.async_delta_widget.event_deltas.len()
                {
                    self.async_delta_widget.start_index += 1;
                }
                if self.consensus_delta_widget.start_index
                    < self.consensus_delta_widget.event_deltas.len()
                {
                    self.consensus_delta_widget.start_index += 1;
                }
            }
            KeyCode::Char('k') if modifiers == KeyModifiers::CONTROL => {
                if self.event_list_widget.start_index > 10 {
                    self.event_list_widget.start_index -= 10;
                } else {
                    self.event_list_widget.start_index = 0;
                }
            }
            KeyCode::Char('j') if modifiers == KeyModifiers::CONTROL => {
                self.event_list_widget.start_index += 10;
                self.event_list_widget.start_index = min(
                    self.event_list_widget.start_index,
                    self.event_list_widget.wrapped_events.len() - 1,
                );
            }
            KeyCode::Char('k') => {
                if self.event_list_widget.start_index > 0 {
                    self.event_list_widget.start_index -= 1;
                }
            }
            KeyCode::Char('j') => {
                self.event_list_widget.start_index += 1;
                self.event_list_widget.start_index = min(
                    self.event_list_widget.start_index,
                    self.event_list_widget.wrapped_events.len() - 1,
                );
            }
            _ => {}
        }
    }

    fn exit(&mut self) {
        self.exit = true;
    }
}

#[derive(Debug)]
pub struct EventCountsWidget {
    wrapped_events: Rc<Vec<WrappedEvent>>,
    counts: HashMap<String, u64>,
}

impl EventCountsWidget {
    pub fn new(wal_events: Rc<Vec<WrappedEvent>>) -> Self {
        let mut s = Self {
            wrapped_events: wal_events,
            counts: HashMap::default(),
        };
        s.load_counts();
        s
    }

    fn load_counts(&mut self) {
        let monad_events: Vec<WalEvent> = self
            .wrapped_events
            .iter()
            .map(|a| a.event.clone())
            .collect();
        self.counts = counter(&monad_events);
    }
}

impl Widget for &EventCountsWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let data = self.counts.iter().map(|(k, v)| (&k[..], *v)).collect_vec();

        BarChart::default()
            .block(Block::default().title("Event Counts").borders(Borders::ALL))
            .bar_width(2)
            .bar_gap(1)
            .bar_style(Style::new().blue())
            .value_style(Style::new().magenta().on_black())
            .data(&data)
            .direction(Direction::Horizontal)
            .render(area, buf);
    }
}

pub struct EventDeltaWidget {
    name: &'static str,
    wrapped_events: Rc<Vec<WrappedEvent>>,
    event_deltas: Vec<i64>,
    start_index: usize,
}

impl EventDeltaWidget {
    pub fn new<F: Fn(&WalEvent) -> bool>(
        name: &'static str,
        wal_events: Rc<Vec<WrappedEvent>>,
        event_matcher: F,
    ) -> Self {
        let mut s = Self {
            name,
            wrapped_events: wal_events,
            event_deltas: Vec::default(),
            start_index: 0,
        };
        s.load_deltas(event_matcher);
        s
    }

    fn load_deltas<F: Fn(&WalEvent) -> bool>(&mut self, event_matcher: F) {
        self.event_deltas = time_between_events_ms(&self.wrapped_events, event_matcher);
    }
}

impl Widget for &EventDeltaWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let data = self.event_deltas[self.start_index..]
            .iter()
            .map(|v| ("", *v as u64))
            .collect_vec();

        BarChart::default()
            .block(Block::default().title(self.name).borders(Borders::ALL))
            .bar_width(3)
            .bar_gap(1)
            .bar_style(Style::new().green())
            .value_style(Style::new().red().on_black())
            .data(&data)
            .direction(Direction::Vertical)
            .render(area, buf);
    }
}

pub struct EventDeltaChart {
    name: &'static str,
    wrapped_events: Rc<Vec<WrappedEvent>>,
    event_deltas: Vec<i64>,
}

impl EventDeltaChart {
    pub fn new<F: Fn(&WalEvent) -> bool>(
        name: &'static str,
        wal_events: Rc<Vec<WrappedEvent>>,
        event_matcher: F,
    ) -> Self {
        let mut s = Self {
            name,
            wrapped_events: wal_events,
            event_deltas: Vec::default(),
        };
        s.load_deltas(event_matcher);
        s
    }

    fn load_deltas<F: Fn(&WalEvent) -> bool>(&mut self, event_matcher: F) {
        self.event_deltas = time_between_events_ms(&self.wrapped_events, event_matcher);
    }
}

impl Widget for &EventDeltaChart {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let async_data = self
            .event_deltas
            .iter()
            .enumerate()
            .map(|(i, v)| (i as f64, *v as f64))
            .collect_vec();

        let num_samples = async_data.len();
        let x_midpoint = num_samples / 2;

        let y_high = async_data.iter().map(|(_, x)| *x).reduce(f64::max).unwrap();
        let y_midpoint = y_high / 2.0;

        let dataset = Dataset::default()
            .name(self.name)
            .marker(symbols::Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().magenta())
            .data(&async_data);

        // Create the X axis and define its properties
        let x_axis = Axis::default()
            .title("sample".red())
            .style(Style::default().white())
            .bounds([0.0, num_samples as f64])
            .labels(vec![
                "0".into(),
                x_midpoint.to_string().into(),
                num_samples.to_string().into(),
            ]);

        // Create the Y axis and define its properties
        let y_axis = Axis::default()
            .title("Time deltas (ms)".red())
            .style(Style::default().white())
            .bounds([0.0, y_high])
            .labels(vec![
                "0.0".into(),
                y_midpoint.to_string().into(),
                y_high.to_string().into(),
            ]);

        Chart::new(vec![dataset])
            .block(Block::default().title("Chart"))
            .x_axis(x_axis)
            .y_axis(y_axis)
            .legend_position(Some(LegendPosition::TopLeft))
            .render(area, buf);
    }
}

pub struct EventListWidget {
    wrapped_events: Rc<Vec<WrappedEvent>>,
    start_index: usize,
}

impl EventListWidget {
    pub fn new(wal_events: Rc<Vec<WrappedEvent>>) -> Self {
        Self {
            wrapped_events: wal_events,
            start_index: 0,
        }
    }
}

impl Widget for &EventListWidget {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let events = self.wrapped_events[self.start_index..].iter().map(|e| {
            let header_str: String = match e.event {
                MonadEvent::ConsensusEvent(_) => "CONSENSUS".to_string(),
                MonadEvent::BlockSyncEvent(_) => "BLOCKSYNC".to_string(),
                MonadEvent::ValidatorEvent(_) => "VALIDATOR".to_string(),
                MonadEvent::MempoolEvent(_) => "MEMPOOL".to_string(),
                MonadEvent::StateRootEvent(_) => "STATEROOT".to_string(),
                MonadEvent::AsyncStateVerifyEvent(_) => "ASYNCSTATEVERIFY".to_string(),
                MonadEvent::ControlPanelEvent(_) => "CONTROLPANEL".to_string(),
                MonadEvent::TimestampUpdateEvent(_) => "TIMESTAMP".to_string(),
                MonadEvent::StateSyncEvent(_) => "STATESYNC".to_string(),
                MonadEvent::DiscoveryEvent(_) => "DISCOVERY".to_string(),
            };

            let s = Span::styled(format!("{header_str:<20}"), Style::default().blue());
            let header = Line::from(vec![
                s,
                " ".repeat(9).into(),
                //e.timestamp.to_string().into(),
                Span::styled(
                    format!("{}", e.timestamp.to_string()),
                    Style::default().yellow(),
                ),
            ]);

            let log = Line::from(e.event.to_string());

            ListItem::new(vec![
                Line::from("-".repeat(area.width as usize)),
                header,
                Line::from(""),
                log,
            ])
        });

        ratatui::widgets::Widget::render(
            List::new(events)
                .block(Block::default().borders(Borders::ALL).title("Events"))
                .direction(ListDirection::TopToBottom),
            area,
            buf,
        );
    }
}

#[derive(Debug, Serialize)]
struct BlockInfo {
    author: String,
    epoch: Epoch,
}

#[derive(Debug, Serialize, Default)]
struct EventStat {
    block_time: BTreeMap<u64, DateTime<Utc>>,
    leader: BTreeMap<u64, BlockInfo>,
}

struct StatExtractor {
    events: Vec<WrappedEvent>,
}

impl StatExtractor {
    fn new(wal_path: PathBuf, start: usize, end: usize) -> Self {
        let logger_config: WALoggerConfig<WrappedEvent> = WALoggerConfig::new(wal_path, true);

        let wal_events = logger_config
            .events()
            .skip(start)
            .take(end.checked_sub(start).expect("end < start"))
            .collect_vec();
        Self { events: wal_events }
    }

    fn extract(&self) -> EventStat {
        let mut stat = EventStat::default();
        for event in &self.events {
            match &event.event {
                MonadEvent::ConsensusEvent(monad_executor_glue::ConsensusEvent::Message {
                    sender: _,
                    unverified_message,
                }) => {
                    let msg = unverified_message.get_obj_unsafe();
                    match &msg.message {
                        ProtocolMessage::Proposal(p) => {
                            let (author, round, epoch) = (
                                format!("{:?}", p.block.author.pubkey()),
                                p.block.get_round(),
                                p.block.epoch,
                            );
                            stat.block_time.insert(round.0, event.timestamp);
                            stat.leader.insert(round.0, BlockInfo { author, epoch });
                        }
                        _ => continue,
                    }
                }
                _ => continue,
            }
        }
        stat
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    assert!(args.start < args.end);

    match args.mode {
        ModeCommand::View => {
            let mut terminal = tui_init()?;
            terminal.clear()?;
            let app_result = MainView::new(args.wal_path, args.start, args.end).run(&mut terminal);

            tui_restore()?;
            app_result
        }
        ModeCommand::Stat(output) => {
            let stat = StatExtractor::new(args.wal_path, args.start, args.end).extract();
            match (output.stdout, output.file) {
                (true, None) => {
                    let writer = BufWriter::new(stdout());
                    serde_json::to_writer(writer, &stat)?;
                }
                (false, Some(file)) => {
                    let file = fs::File::create(file)?;
                    let writer = BufWriter::new(file);
                    serde_json::to_writer(writer, &stat)?;
                }
                _ => unreachable!(),
            }
            Ok(())
        }
    }
}

fn counter(events: &Vec<WalEvent>) -> HashMap<String, u64> {
    let mut buckets = HashMap::new();
    buckets.entry("consensusevent".into()).or_default();
    buckets.entry("blocksyncevent".into()).or_default();
    buckets.entry("validatorevent".into()).or_default();
    buckets.entry("mempoolevent".into()).or_default();
    buckets.entry("asyncstateverifyevent".into()).or_default();
    buckets.entry("metricsevent".into()).or_default();

    for e in events {
        let key = match e {
            MonadEvent::ConsensusEvent(_) => "consensusevent",
            MonadEvent::BlockSyncEvent(_) => "blocksyncevent",
            MonadEvent::ValidatorEvent(_) => "validatorevent",
            MonadEvent::MempoolEvent(_) => "mempoolevent",
            MonadEvent::StateRootEvent(_) => "staterootevent",
            MonadEvent::AsyncStateVerifyEvent(_) => "asyncstateverifyevent",
            WalEvent::ControlPanelEvent(_) => "controlpanelevent",
            MonadEvent::TimestampUpdateEvent(_) => "timestampupdateevent",
            MonadEvent::StateSyncEvent(_) => "statesyncevent",
            MonadEvent::DiscoveryEvent(_) => "discoveryevent",
        };

        buckets
            .entry(key.into())
            .and_modify(|c| *c += 1)
            .or_insert(0);
    }

    buckets
}

fn time_between_events_ms<F: Fn(&WalEvent) -> bool>(
    events: &[WrappedEvent],
    event_matcher: F,
) -> Vec<i64> {
    let asyncs: Vec<_> = events
        .iter()
        .filter(|&e| event_matcher(&e.event))
        .tuple_windows()
        .map(|(x, y)| y.timestamp - x.timestamp)
        .collect();

    asyncs
        .iter()
        .map(TimeDelta::num_milliseconds)
        .collect::<Vec<_>>()
}
