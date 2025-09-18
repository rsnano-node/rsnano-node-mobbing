use std::sync::{Arc, Mutex};

use rsnano_messages::{Message, Preproposal, Proposal, ProposalVote};
use rsnano_network::TrafficType;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};

use crate::transport::MessageFlooder;

pub struct Networking {
    flooder: Mutex<MessageFlooder>,
    receive_preproposal_listener: OutputListenerMt<Preproposal>,
}

impl Networking {
    pub(crate) fn new(flooder: MessageFlooder) -> Self {
        Self { 
            flooder: flooder.into(), 
            receive_preproposal_listener: OutputListenerMt::new(),
        }
    }

    pub(crate) fn publish_message(&self, message: &Message) {
        self.flooder.lock().unwrap().flood_prs_and_some_non_prs(
            message,
            TrafficType::LedgerSnapshots,
            0.0,
        );
    }

    pub(crate) fn track_received_preproposals(&self) -> Arc<OutputTrackerMt<Preproposal>> {
        self.receive_preproposal_listener.track()
    }

    pub(crate) fn receive_preproposal(&self, preproposal: Preproposal) {
        self.receive_preproposal_listener.emit(preproposal);
    }
}

#[cfg(test)]
mod tests {
    use rsnano_messages::Preproposal;
    use crate::{ledger_snapshots::network::Networking, transport::MessageFlooder};


    #[test]
    fn can_track_received_preproposals() {
        let network = Networking::new(MessageFlooder::new_null());
        let receive_preproposal_tracker = network.track_received_preproposals();
        
        let preproposal = Preproposal::new_test_instance();
        
        network.receive_preproposal(preproposal.clone());

        let receive_events = receive_preproposal_tracker.output();

        assert_eq!(receive_events.len(), 1, "Should receive preproposal");
        assert_eq!(receive_events[0], preproposal);
    }

}