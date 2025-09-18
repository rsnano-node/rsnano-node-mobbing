use crate::{
    ledger_snapshots::{Aggregator, tally::find_winner_proposal},
    representatives::ConsensusParams,
};
use rsnano_messages::{Aggregatable, Preproposal, Proposal, ProposalVote};
use rsnano_types::{PrivateKey, SnapshotNumber};

#[derive(Default)]
pub(crate) struct LedgerSnapshotsState {
    pub(crate) preproposal_aggregator: Aggregator<Preproposal>,
    pub(crate) proposal_aggregator: Aggregator<Proposal>,
    pub(crate) vote_aggregator: Aggregator<ProposalVote>,
    pub(crate) proposal_voted: bool,
    pub(crate) current_snapshot_number: u32,
}

impl LedgerSnapshotsState {
    pub(crate) fn add_preproposal(&mut self, preproposal: Preproposal) -> bool {
        if preproposal.snapshot_number != self.current_snapshot_number {
            return false;
        }
        self.preproposal_aggregator.add(preproposal);
        true
    }

    pub(crate) fn try_create_proposal(
        &self,
        consensus_params: &ConsensusParams,
        rep_key: &PrivateKey,
    ) -> Option<Proposal> {
        if self.preproposal_aggregator.has_quorum(consensus_params) {
            let proposal = Proposal::new(
                self.preproposal_aggregator.values(),
                rep_key,
                self.current_snapshot_number,
            );
            Some(proposal)
        } else {
            None
        }
    }

    pub(crate) fn receive_proposal(&mut self, proposal: Proposal) -> bool {
        if proposal.snapshot_number != self.current_snapshot_number {
            return false;
        }

        self.proposal_aggregator.add(proposal);
        true
    }

    pub(crate) fn try_create_vote(
        &mut self,
        consensus_params: &ConsensusParams,
        rep_key: &PrivateKey,
    ) -> Option<ProposalVote> {
        let has_quorum = self.proposal_aggregator.has_quorum(&consensus_params);

        if has_quorum && !self.proposal_voted {
            let proposal_vote = create_proposal_vote(
                &self.proposal_aggregator,
                rep_key,
                self.current_snapshot_number,
            )
            .expect("Should always be able to create a vote when quorum reached");
            self.proposal_voted = true;
            Some(proposal_vote)
        } else {
            None
        }
    }

    pub(crate) fn receive_vote(
        &mut self,
        vote: ProposalVote,
        consensus_params: &ConsensusParams,
    ) -> bool {
        if vote.snapshot_number != self.current_snapshot_number {
            return false;
        }

        self.vote_aggregator.add(vote);

        if let Some(winner) = find_winner_proposal(&consensus_params, self.vote_aggregator.values())
        {
            tracing::warn!(proposal_hash=?winner, "Found a winner!");
            self.current_snapshot_number += 1;
            self.preproposal_aggregator.clear();
            self.proposal_aggregator.clear();
            self.vote_aggregator.clear();
        }

        true
    }
}

pub(crate) fn create_proposal_vote(
    aggregator: &Aggregator<Proposal>,
    private_key: &PrivateKey,
    snapshot_number: SnapshotNumber,
) -> Option<ProposalVote> {
    Some(ProposalVote::new(
        aggregator.values().map(|p| p.hash()).max()?,
        private_key,
        snapshot_number,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_ledger::RepWeights;
    use rsnano_messages::ProposalHash;
    use rsnano_types::Amount;

    #[test]
    fn discard_preproposal_with_different_snapshot_number_than_current() {
        let mut state = LedgerSnapshotsState::default();
        state.current_snapshot_number = 10;

        let preproposal1 = Preproposal::new(
            vec![],
            &PrivateKey::from(1),
            state.current_snapshot_number - 1,
        );
        state.add_preproposal(preproposal1.clone());

        assert!(state.preproposal_aggregator.is_empty());

        let preproposal2 = Preproposal::new(
            vec![],
            &PrivateKey::from(1),
            state.current_snapshot_number + 1,
        );
        state.add_preproposal(preproposal2.clone());

        assert!(state.preproposal_aggregator.is_empty());
    }

    #[test]
    fn discard_proposal_with_different_snapshot_number_than_current() {
        let mut state = LedgerSnapshotsState::default();
        state.current_snapshot_number = 10;

        let proposal1 = Proposal::new(
            vec![],
            &PrivateKey::from(1),
            state.current_snapshot_number - 1,
        );
        state.receive_proposal(proposal1.clone());

        assert!(state.proposal_aggregator.is_empty());

        let proposal2 = Proposal::new(
            vec![],
            &PrivateKey::from(1),
            state.current_snapshot_number + 1,
        );
        state.receive_proposal(proposal2.clone());

        assert!(state.proposal_aggregator.is_empty());
    }

    #[test]
    fn discard_proposal_vote_with_different_snapshot_number_than_current() {
        let mut state = LedgerSnapshotsState::default();
        state.current_snapshot_number = 10;
        let snapshot_number = state.current_snapshot_number;

        let proposal_vote1 = ProposalVote::new(
            ProposalHash::from(1),
            &PrivateKey::from(1),
            snapshot_number - 1,
        );

        state.receive_vote(proposal_vote1, &ConsensusParams::default());

        assert!(state.vote_aggregator.is_empty());

        let proposal_vote2 = ProposalVote::new(
            ProposalHash::from(1),
            &PrivateKey::from(1),
            snapshot_number + 1,
        );

        state.receive_vote(proposal_vote2, &ConsensusParams::default());

        assert!(state.vote_aggregator.is_empty());
    }

    #[test]
    fn current_snapshot_number_is_increased_when_proposal_gets_confirmed() {
        let rep_key = PrivateKey::from(1);
        let mut weights = RepWeights::new();
        weights.insert(rep_key.public_key(), Amount::MAX);

        let mut state = LedgerSnapshotsState::default();
        let snapshot_number = state.current_snapshot_number;

        let preproposal = Preproposal::new(vec![], &rep_key, snapshot_number);
        let proposal = Proposal::new([&preproposal], &rep_key, snapshot_number);
        let proposal_vote = ProposalVote::new(ProposalHash::from(123), &rep_key, snapshot_number);

        state.add_preproposal(preproposal);
        state.receive_proposal(proposal);
        let consensus_params = ConsensusParams {
            rep_weights: weights,
            ..Default::default()
        };
        state.receive_vote(proposal_vote, &consensus_params);

        assert_eq!(state.current_snapshot_number, snapshot_number + 1);
        assert_eq!(
            state.preproposal_aggregator.len(),
            0,
            "preproposals not cleared"
        );
        assert_eq!(state.proposal_aggregator.len(), 0, "proposals not cleared");
        assert_eq!(state.vote_aggregator.len(), 0, "votes not cleared");
    }
}
