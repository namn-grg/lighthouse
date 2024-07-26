use crate::beacon_chain::BeaconChainTypes;
use crate::validator_pubkey_cache::DatabasePubkey;
use slog::error;
use slog::{info, Logger};
use ssz::{Decode, Encode};
use std::sync::Arc;
use store::{
    chunked_vector::{BlockRoots, Field},
    get_key_for_col, DBColumn, Error, HotColdDB, KeyValueStore, KeyValueStoreOp, StoreItem,
};
use types::{Hash256, PublicKey};

pub fn upgrade_to_v21<T: BeaconChainTypes>(
    db: Arc<HotColdDB<T::EthSpec, T::HotStore, T::ColdStore>>,
    log: Logger,
) -> Result<Vec<KeyValueStoreOp>, Error> {
    info!(log, "Upgrading from v21 to v22");

    let anchor = db.get_anchor_info();
    let split_slot = db.get_split_slot();

    if !db.get_config().allow_tree_states_migration && !anchor.no_historic_states_stored(split_slot)
    {
        error!(
            log,
            "You are attempting to migrate to tree-states but this is a destructive operation. \
             Upgrading will require FIXME(sproul) minutes of downtime before Lighthouse starts again. \
             All current historic states will be deleted. Reconstructing the states in the new \
             schema will take up to 2 weeks. \
             \
             To proceed add the flag --allow-tree-states-migration OR run lighthouse db prune-states"
        );
        return Err(Error::DestructiveFreezerUpgrade);
    }

    let mut ops = vec![];

    rewrite_block_roots(&db, anchor.oldest_block_slot, &log, &mut ops)?;

    // TODO:
    // delete all other "chunked vector" columns: BeaconStateRoots, BeaconRandao, HistoricSummaries, etc
    // we can do this by calling `prune_historic_states`

    Ok(ops)
}

pub fn rewrite_block_roots<T: BeaconChainTypes>(
    db: &HotColdDB<T::EthSpec, T::HotStore, T::ColdStore>,
    oldest_block_slot: Slot,
    split_slot: Slot,
    log: &Logger,
    ops: &mut Vec<KeyValueStoreOp>,
) -> Result<(), Error> {
    // Block roots are available from the `oldest_block_slot` to the `split_slot`.
    let start_vindex = oldest_block_slot.as_usize();
    let block_root_iter = ChunkedVectorIter::<BlockRoots, _, _, _>::new(
        &db,
        start_vindex,
        split_slot,
        db.get_chain_spec(),
    );

    // OK to hold these in memory (10M slots * 43 bytes per KV ~= 430 MB).
    for (slot, block_root) in block_root_iter {
        ops.push(KeyValueStoreOp::PutValue(
            get_key_for_col(DBColumn::BeaconBlockRoots, &(slot as u64).as_le_bytes()),
            block_root.as_slice(),
        ))?;
    }

    Ok(())
}
