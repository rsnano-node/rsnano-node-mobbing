mod aggregator;
mod ledger_snapshots_state;
mod tally;
mod network;

use std::sync::{Arc, Mutex};

use rsnano_ledger::Ledger;
use rsnano_messages::{Aggregatable, Message, Preproposal, Proposal, ProposalVote};
use rsnano_network::TrafficType;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use rsnano_types::{Account, BlockHash};
use rsnano_types::{PrivateKey, SnapshotNumber};

use crate::ledger_snapshots::network::Networking;
use crate::{
    ledger_snapshots::{aggregator::Aggregator, ledger_snapshots_state::LedgerSnapshotsState},
    representatives::OnlineReps,
    transport::MessageFlooder,
};
use tracing::warn;

pub struct LedgerSnapshots {
    ledger: Arc<Ledger>,
    /// For simplicity we currently assume that there is at most
    /// one representative key!
    /// TODO: We have to extend this later to multiple representatives per node.
    get_private_key: Box<dyn Fn() -> Option<PrivateKey> + Send + Sync>,
    network: Networking,
    receive_proposal_listener: OutputListenerMt<Proposal>,
    receive_proposal_vote_listener: OutputListenerMt<ProposalVote>,
    state: Mutex<LedgerSnapshotsState>,

    online_reps: Arc<Mutex<OnlineReps>>,
}

impl LedgerSnapshots {
    pub fn new(
        ledger: Arc<Ledger>,
        get_private_key: impl Fn() -> Option<PrivateKey> + Send + Sync + 'static,
        flooder: MessageFlooder,
        online_reps: Arc<Mutex<OnlineReps>>,
    ) -> Self {
        Self {
            ledger,
            get_private_key: Box::new(get_private_key),
            network: Networking::new(flooder).into(),
            receive_proposal_listener: OutputListenerMt::new(),
            receive_proposal_vote_listener: OutputListenerMt::new(),
            state: Default::default(),
            online_reps,
        }
    }

    pub fn new_null() -> Self {
        Self::new(
            Ledger::new_null().into(),
            || None,
            MessageFlooder::new_null(),
            Mutex::new(OnlineReps::default()).into(),
        )
    }

    pub fn start_ledger_snapshot(&self) {
        warn!("Preproposal generation triggered");
        // TODO add test for no private key
        let private_key = (self.get_private_key)().unwrap();
        let preproposal = self.create_preproposal(&private_key);
        let message = Message::SnapshotPreproposal(preproposal);
        self.network.publish_message(&message);
    }

    fn create_preproposal(&self, private_key: &PrivateKey) -> Preproposal {
        let frontiers = self.collect_frontiers();
        Preproposal::new(frontiers, private_key, self.get_current_snapshot_number())
    }

    fn collect_frontiers(&self) -> Vec<(Account, BlockHash)> {
        self.ledger.confirmed().frontiers().collect()
    }

    pub fn handle_preproposal(&self, preproposal: Preproposal) {
        warn!(preproposal_hash= ?preproposal.hash(), "Snapshot preproposal received");
        self.network.receive_preproposal(preproposal.clone());
        
        let consensus_params = self.online_reps.lock().unwrap().get_consensus_params();
        let mut state = self.state.lock().unwrap();
        
        if !state.add_preproposal(preproposal.clone()) {
            warn!(preproposal_hash= ?preproposal.hash(), snapshot_number= ?preproposal.snapshot_number, "Snapshot preproposal discarded because snapshot number is different than current");
            return;
        }

        warn!(
            "Current preproposal tally = {:?}",
            state.preproposal_aggregator.tally(&consensus_params)
        );

        let rep_key = (self.get_private_key)().unwrap();
        let proposal = state.try_create_proposal(&consensus_params, &rep_key);
        if proposal.is_some() {
            warn!("Quorum on preproposals reached");
        } else {
            warn!("No quorum on preproposals yet");
        }
        drop(state);

        if let Some(proposal) = proposal {
            warn!(proposal_hash = ?proposal.hash(), "Created proposal. Flooding...");
            self.network.publish_message(&Message::SnapshotProposal(proposal));
        };
    }

    pub fn receive_proposal(&self, proposal: Proposal) {
        warn!(proposal_hash = ?proposal.hash(), "Snapshot proposal received");
        self.receive_proposal_listener.emit(proposal.clone());
        let consensus_params = self.online_reps.lock().unwrap().get_consensus_params();

        let mut state = self.state.lock().unwrap();
        if !state.receive_proposal(proposal.clone()) {
            warn!(proposal_hash= ?proposal.hash(), snapshot_number= ?proposal.snapshot_number, "Snapshot proposal discarded because snapshot number is different than current");
            return;
        }

        warn!(
            "Current proposal tally = {:?}",
            state.proposal_aggregator.tally(&consensus_params)
        );

        let rep_key = (self.get_private_key)().unwrap();
        if let Some(vote) = state.try_create_vote(&consensus_params, &rep_key) {
            warn!("Quorum on proposal reached");
            warn!(vote_hash = ?vote.hash(), "Flooding proposal vote");
            self.network.publish_message(&Message::SnapshotProposalVote(vote));
        }
    }

    pub fn track_received_preproposals(&self) -> Arc<OutputTrackerMt<Preproposal>> {
        self.network.track_received_preproposals()
    }

    pub fn track_received_proposals(&self) -> Arc<OutputTrackerMt<Proposal>> {
        self.receive_proposal_listener.track()
    }

    pub fn track_received_proposal_votes(&self) -> Arc<OutputTrackerMt<ProposalVote>> {
        self.receive_proposal_vote_listener.track()
    }

    pub fn receive_proposal_vote(&self, proposal_vote: ProposalVote) {
        self.receive_proposal_vote_listener
            .emit(proposal_vote.clone());

        let consensus_params = self.online_reps.lock().unwrap().get_consensus_params();
        let mut state = self.state.lock().unwrap();

        if !state.receive_vote(proposal_vote.clone(), &consensus_params) {
            warn!(proposal_vote_hash= ?proposal_vote.hash(), snapshot_number= ?proposal_vote.snapshot_number, "Snapshot proposal vote discarded because snapshot number is different than current");
            return;
        }

        warn!(
            received_votes = state.vote_aggregator.len(),
            "Snapshot proposal vote received"
        );
    }

    fn get_current_snapshot_number(&self) -> SnapshotNumber {
        self.state.lock().unwrap().current_snapshot_number
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ledger_snapshots::ledger_snapshots_state::create_proposal_vote,
        representatives::ONLINE_WEIGHT_QUORUM, transport::FloodEvent,
    };
    use rsnano_ledger::RepWeights;
    use rsnano_messages::{Aggregatable, Message, ProposalHash, ProposalVote};
    use rsnano_network::TrafficType;
    use rsnano_output_tracker::OutputTrackerMt;
    use rsnano_types::{AccountInfo, Amount, ConfirmationHeightInfo};
    use std::time::Duration;

    #[test]
    fn ledger_with_one_account() {
        let account = Account::from(1);
        let frontier = BlockHash::from(2);
        let fixture = Fixture::with_frontiers([(account, frontier)]);
        assert_eq!(fixture.snapshots.collect_frontiers(), [(account, frontier)]);
    }

    #[test]
    fn ledger_with_multiple_accounts() {
        let account1 = Account::from(1);
        let frontier1 = BlockHash::from(100);
        let account2 = Account::from(2);
        let frontier2 = BlockHash::from(200);

        let fixture = Fixture::with_frontiers([(account1, frontier1), (account2, frontier2)]);
        assert_eq!(
            fixture.snapshots.collect_frontiers(),
            [(account1, frontier1), (account2, frontier2)]
        );
    }

    #[test]
    fn create_preproposal() {
        let account = Account::from(10);
        let frontier = BlockHash::from(2);
        let fixture = Fixture::with_frontiers([(account, frontier)]);

        let preproposal = fixture.snapshots.create_preproposal(&PrivateKey::from(1));

        assert!(preproposal.frontiers.contains(&(account, frontier)));
        assert_eq!(
            preproposal.snapshot_number,
            fixture.snapshots.get_current_snapshot_number()
        );
    }

    #[test]
    fn publish_preproposal() {
        let account = Account::from(1);
        let frontier = BlockHash::from(100);
        let fixture = Fixture::with_frontiers([(account, frontier)]);

        fixture.snapshots.start_ledger_snapshot();

        let flood_events = fixture.flood_tracker.output();
        assert_eq!(flood_events.len(), 1, "Should flood the message");

        let expected_preproposal = fixture
            .snapshots
            .create_preproposal(&get_test_key().unwrap());

        assert_eq!(
            flood_events[0],
            FloodEvent {
                message: Message::SnapshotPreproposal(expected_preproposal),
                // TODO: add new traffic type for snapshots
                traffic_type: TrafficType::LedgerSnapshots,
                scale: 0.0,
                all_prs: true,
            }
        );
    }

    #[test]
    fn a_received_preproposal_is_added_to_the_preproposal_aggregator() {
        let fixture = Fixture::new();
        let snapshots = &fixture.snapshots;
        let snapshot_number = snapshots.get_current_snapshot_number();
        let preproposal = Preproposal::new(vec![], &PrivateKey::from(1), snapshot_number);

        snapshots.handle_preproposal(preproposal.clone());

        assert!(
            snapshots
                .state
                .lock()
                .unwrap()
                .preproposal_aggregator
                .contains(&preproposal.hash())
        );
    }

    #[test]
    fn publish_proposal_when_quorum_of_preproposals_is_reached() {
        let mut rep_weights = RepWeights::new();
        let private_key = get_test_key().unwrap();
        let quorum_weight = Amount::nano(100_000);

        rep_weights.insert(private_key.public_key(), quorum_weight);
        let fixture = Fixture::with_rep_weights(rep_weights, quorum_weight);
        let snapshot_number = fixture.snapshots.get_current_snapshot_number();

        let preproposal = Preproposal::new(vec![], &private_key, snapshot_number);
        fixture.snapshots.handle_preproposal(preproposal.clone());

        let flood_events = fixture.flood_tracker.output();
        assert_eq!(flood_events.len(), 1, "Should flood the message");

        let snapshot_number = fixture.snapshots.get_current_snapshot_number();
        let expected_proposal = Proposal::new(&[preproposal], &private_key, snapshot_number);

        assert_eq!(
            flood_events[0],
            FloodEvent {
                message: Message::SnapshotProposal(expected_proposal),
                traffic_type: TrafficType::LedgerSnapshots,
                scale: 0.0,
                all_prs: true,
            }
        );

        assert_eq!(
            snapshot_number,
            fixture.snapshots.get_current_snapshot_number()
        );
    }

    #[test]
    fn can_track_received_proposals() {
        let fixture = Fixture::new();
        let proposal = Proposal::new_test_instance();
        fixture.snapshots.receive_proposal(proposal.clone());

        let receive_events = fixture.receive_proposal_tracker.output();
        assert_eq!(receive_events.len(), 1, "Should receive proposal");
        assert_eq!(receive_events[0], proposal);
    }

    #[test]
    fn a_received_proposal_is_added_to_the_proposal_aggregator() {
        let fixture = Fixture::new();
        let snapshots = &fixture.snapshots;
        let proposal = Proposal::new(
            vec![],
            &PrivateKey::from(1),
            snapshots.get_current_snapshot_number(),
        );

        snapshots.receive_proposal(proposal.clone());

        assert!(
            snapshots
                .state
                .lock()
                .unwrap()
                .proposal_aggregator
                .contains(&proposal.hash())
        );
    }

    #[test]
    fn publish_proposal_vote_when_quorum_of_proposals_is_reached() {
        let mut rep_weights = RepWeights::new();
        let private_key = get_test_key().unwrap();
        let quorum_weight = Amount::nano(100_000);

        rep_weights.insert(private_key.public_key(), quorum_weight);
        let fixture = Fixture::with_rep_weights(rep_weights, quorum_weight);
        let snapshot_number = fixture.snapshots.get_current_snapshot_number();

        let proposal = Proposal::new(vec![], &private_key, snapshot_number);
        fixture.snapshots.receive_proposal(proposal.clone());

        let flood_events = fixture.flood_tracker.output();
        assert_eq!(flood_events.len(), 1, "Should flood the message");

        let expected_proposal_vote =
            ProposalVote::new(proposal.hash(), &private_key, snapshot_number);

        assert_eq!(
            flood_events[0],
            FloodEvent {
                message: Message::SnapshotProposalVote(expected_proposal_vote),
                traffic_type: TrafficType::LedgerSnapshots,
                scale: 0.0,
                all_prs: true,
            }
        );

        assert_eq!(
            snapshot_number,
            fixture.snapshots.get_current_snapshot_number()
        );
    }

    #[test]
    fn publish_proposal_vote_only_once() {
        let mut rep_weights = RepWeights::new();
        let private_key = get_test_key().unwrap();
        let quorum_weight = Amount::nano(100_000);
        rep_weights.insert(private_key.public_key(), quorum_weight);

        let fixture = Fixture::with_rep_weights(rep_weights, quorum_weight);
        let snapshot_number = fixture.snapshots.get_current_snapshot_number();

        let proposal1 = Proposal::new(vec![], &private_key, snapshot_number);
        let proposal2 = Proposal::new(vec![], &PrivateKey::from(2), snapshot_number);
        fixture.snapshots.receive_proposal(proposal1.clone());
        fixture.snapshots.receive_proposal(proposal2);

        let flood_events = fixture.flood_tracker.output();
        assert_eq!(flood_events.len(), 1, "Should flood only one vote message");
    }

    #[test]
    fn vote_for_proposal_with_highest_hash() {
        let snapshot_number = 0;
        let proposal1 = Proposal::new(vec![], &PrivateKey::from(1), snapshot_number);
        let proposal2 = Proposal::new(vec![], &PrivateKey::from(2), snapshot_number);
        let proposal3 = Proposal::new(vec![], &PrivateKey::from(3), snapshot_number);
        let proposal4 = Proposal::new(vec![], &PrivateKey::from(4), snapshot_number);

        let highest_hash = [
            proposal1.hash(),
            proposal2.hash(),
            proposal3.hash(),
            proposal4.hash(),
        ]
        .into_iter()
        .max()
        .unwrap();

        let mut proposal_aggregator = Aggregator::<Proposal>::default();
        proposal_aggregator.add(proposal1);
        proposal_aggregator.add(proposal2);
        proposal_aggregator.add(proposal3);
        proposal_aggregator.add(proposal4);

        let proposal_vote =
            create_proposal_vote(&proposal_aggregator, &PrivateKey::from(5), snapshot_number);

        assert_eq!(proposal_vote.unwrap().proposal_hash, highest_hash);
    }

    #[test]
    fn can_track_received_proposal_votes() {
        let fixture = Fixture::new();
        let proposal_vote = ProposalVote::new_test_instance();
        fixture
            .snapshots
            .receive_proposal_vote(proposal_vote.clone());

        let receive_events = fixture.receive_proposal_vote_tracker.output();
        assert_eq!(receive_events.len(), 1, "Should receive proposal vote");
        assert_eq!(receive_events[0], proposal_vote);
    }

    #[test]
    fn a_received_proposal_vote_is_added_to_the_proposal_vote_aggregator() {
        let fixture = Fixture::new();
        let snapshots = &fixture.snapshots;
        let proposal_vote = ProposalVote::new(
            ProposalHash::from(1),
            &PrivateKey::from(1),
            snapshots.get_current_snapshot_number(),
        );

        snapshots.receive_proposal_vote(proposal_vote.clone());

        assert!(
            snapshots
                .state
                .lock()
                .unwrap()
                .vote_aggregator
                .contains(&proposal_vote.hash())
        );
    }

    #[test]
    fn initial_snapshot_number_should_be_zero() {
        let ledger_snapshots = LedgerSnapshots::new_null();

        assert_eq!(ledger_snapshots.get_current_snapshot_number(), 0);
    }

    struct Fixture {
        snapshots: LedgerSnapshots,
        flood_tracker: Arc<OutputTrackerMt<FloodEvent>>,
        receive_proposal_tracker: Arc<OutputTrackerMt<Proposal>>,
        receive_proposal_vote_tracker: Arc<OutputTrackerMt<ProposalVote>>,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_frontiers([])
        }

        fn with_frontiers(frontiers: impl IntoIterator<Item = (Account, BlockHash)>) -> Self {
            let ledger = create_ledger_with_frontiers(frontiers);
            Self::with_ledger(ledger)
        }

        fn with_ledger(ledger: Arc<Ledger>) -> Self {
            Self::with_ledger_and_weights(ledger, RepWeights::new(), Amount::nano(60_000_000))
        }

        fn with_rep_weights(rep_weights: RepWeights, quorum_weight: Amount) -> Self {
            let ledger = create_ledger_with_frontiers([]);
            Self::with_ledger_and_weights(ledger, rep_weights, quorum_weight)
        }

        fn with_ledger_and_weights(
            ledger: Arc<Ledger>,
            rep_weights: RepWeights,
            quorum_weight: Amount,
        ) -> Self {
            let flooder = MessageFlooder::new_null();
            let flood_tracker = flooder.track_floods();

            let mut online_reps = OnlineReps::new(
                Arc::new(rep_weights.into()),
                Duration::ZERO,
                Amount::ZERO,
                Amount::ZERO,
            );
            online_reps.set_trended(quorum_weight / ONLINE_WEIGHT_QUORUM as u128 * 100);
            let online_reps = Arc::new(Mutex::new(online_reps));

            let snapshots =
                LedgerSnapshots::new(ledger.clone(), get_test_key, flooder, online_reps);

            snapshots.state.lock().unwrap().current_snapshot_number = 10;

            let receive_proposal_tracker = snapshots.track_received_proposals();
            let receive_proposal_vote_tracker = snapshots.track_received_proposal_votes();

            Self {
                snapshots,
                flood_tracker,
                receive_proposal_tracker,
                receive_proposal_vote_tracker,
            }
        }
    }

    fn get_test_key() -> Option<PrivateKey> {
        Some(PrivateKey::from(123))
    }

    fn create_ledger_with_frontiers(
        frontiers: impl IntoIterator<Item = (Account, BlockHash)>,
    ) -> Arc<Ledger> {
        let mut builder = Ledger::new_null_builder();

        for (account, frontier) in frontiers {
            builder = builder
                .account_info(&account, &AccountInfo::new_test_instance())
                .confirmation_height(
                    &account,
                    &ConfirmationHeightInfo {
                        height: 0,
                        frontier,
                    },
                );
        }

        builder.finish().into()
    }
}
