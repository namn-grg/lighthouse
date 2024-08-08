use crate::{slot_epoch::Slot, Address, EthSpec, Transactions};

pub struct InclusionRequest<E: EthSpec> {
    pub slot: Slot,
    pub proposer_index: usize,
    pub entries: Transactions<E>,
}

pub struct InclusionListAggregatedEntry {
    pub from_address: Address,
    pub gas_limit: u64,
    pub bitlist: Vec<u8>,
}

pub struct InclusionListAggregated {
    pub slot: Slot,
    pub proposer_index: usize,
    pub message: Vec<InclusionListAggregatedEntry>,
}
