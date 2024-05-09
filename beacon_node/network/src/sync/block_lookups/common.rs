use crate::sync::block_lookups::single_block_lookup::{
    LookupRequestError, SingleBlockLookup, SingleLookupRequestState,
};
use crate::sync::block_lookups::{BlobRequestState, BlockRequestState, PeerId};
use crate::sync::manager::{BlockProcessType, Id, SLOT_IMPORT_TOLERANCE};
use crate::sync::network_context::SyncNetworkContext;
use beacon_chain::block_verification_types::RpcBlock;
use beacon_chain::data_column_verification::CustodyDataColumn;
use beacon_chain::BeaconChainTypes;
use std::sync::Arc;
use types::blob_sidecar::FixedBlobSidecarList;
use types::SignedBeaconBlock;

use super::single_block_lookup::DownloadResult;
use super::SingleLookupId;

use super::single_block_lookup::CustodyRequestState;

#[derive(Debug, Copy, Clone)]
pub enum ResponseType {
    Block,
    Blob,
    CustodyColumn,
}

/// The maximum depth we will search for a parent block. In principle we should have sync'd any
/// canonical chain to its head once the peer connects. A chain should not appear where it's depth
/// is further back than the most recent head slot.
pub(crate) const PARENT_DEPTH_TOLERANCE: usize = SLOT_IMPORT_TOLERANCE * 2;

/// This trait unifies common single block lookup functionality across blocks and blobs. This
/// includes making requests, verifying responses, and handling processing results. A
/// `SingleBlockLookup` includes both a `BlockRequestState` and a `BlobRequestState`, this trait is
/// implemented for each.
///
/// The use of the `ResponseType` associated type gives us a degree of type
/// safety when handling a block/blob response ensuring we only mutate the correct corresponding
/// state.
pub trait RequestState<T: BeaconChainTypes> {
    /// The type created after validation.
    type VerifiedResponseType: Clone;

    /// Request the network context to prepare a request of a component of `block_root`. If the
    /// request is not necessary because the component is already known / processed, return false.
    /// Return true if it sent a request and we can expect an event back from the network.
    fn make_request(
        &self,
        id: Id,
        peer_id: PeerId,
        downloaded_block_expected_blobs: Option<usize>,
        cx: &mut SyncNetworkContext<T>,
    ) -> Result<bool, LookupRequestError>;

    /* Response handling methods */

    /// Send the response to the beacon processor.
    fn send_for_processing(
        id: Id,
        result: DownloadResult<Self::VerifiedResponseType>,
        cx: &SyncNetworkContext<T>,
    ) -> Result<(), LookupRequestError>;

    /* Utility methods */

    /// Returns the `ResponseType` associated with this trait implementation. Useful in logging.
    fn response_type() -> ResponseType;

    /// A getter for the `BlockRequestState` or `BlobRequestState` associated with this trait.
    fn request_state_mut(request: &mut SingleBlockLookup<T>) -> &mut Self;

    /// A getter for a reference to the `SingleLookupRequestState` associated with this trait.
    fn get_state(&self) -> &SingleLookupRequestState<Self::VerifiedResponseType>;

    /// A getter for a mutable reference to the SingleLookupRequestState associated with this trait.
    fn get_state_mut(&mut self) -> &mut SingleLookupRequestState<Self::VerifiedResponseType>;
}

impl<T: BeaconChainTypes> RequestState<T> for BlockRequestState<T::EthSpec> {
    type VerifiedResponseType = Arc<SignedBeaconBlock<T::EthSpec>>;

    fn make_request(
        &self,
        id: SingleLookupId,
        peer_id: PeerId,
        _: Option<usize>,
        cx: &mut SyncNetworkContext<T>,
    ) -> Result<bool, LookupRequestError> {
        cx.block_lookup_request(id, peer_id, self.requested_block_root)
            .map_err(LookupRequestError::SendFailed)
    }

    fn send_for_processing(
        id: SingleLookupId,
        download_result: DownloadResult<Self::VerifiedResponseType>,
        cx: &SyncNetworkContext<T>,
    ) -> Result<(), LookupRequestError> {
        let DownloadResult {
            value,
            block_root,
            seen_timestamp,
            ..
        } = download_result;
        cx.send_block_for_processing(
            block_root,
            RpcBlock::new_without_blobs(Some(block_root), value),
            seen_timestamp,
            BlockProcessType::SingleBlock { id },
        )
        .map_err(LookupRequestError::SendFailed)
    }

    fn response_type() -> ResponseType {
        ResponseType::Block
    }
    fn request_state_mut(request: &mut SingleBlockLookup<T>) -> &mut Self {
        &mut request.block_request_state
    }
    fn get_state(&self) -> &SingleLookupRequestState<Self::VerifiedResponseType> {
        &self.state
    }
    fn get_state_mut(&mut self) -> &mut SingleLookupRequestState<Self::VerifiedResponseType> {
        &mut self.state
    }
}

impl<T: BeaconChainTypes> RequestState<T> for BlobRequestState<T::EthSpec> {
    type VerifiedResponseType = FixedBlobSidecarList<T::EthSpec>;

    fn make_request(
        &self,
        id: Id,
        peer_id: PeerId,
        downloaded_block_expected_blobs: Option<usize>,
        cx: &mut SyncNetworkContext<T>,
    ) -> Result<bool, LookupRequestError> {
        cx.blob_lookup_request(
            id,
            peer_id,
            self.block_root,
            downloaded_block_expected_blobs,
        )
        .map_err(LookupRequestError::SendFailed)
    }

    fn send_for_processing(
        id: Id,
        download_result: DownloadResult<Self::VerifiedResponseType>,
        cx: &SyncNetworkContext<T>,
    ) -> Result<(), LookupRequestError> {
        let DownloadResult {
            value,
            block_root,
            seen_timestamp,
            ..
        } = download_result;
        cx.send_blobs_for_processing(
            block_root,
            value,
            seen_timestamp,
            BlockProcessType::SingleBlob { id },
        )
        .map_err(LookupRequestError::SendFailed)
    }

    fn response_type() -> ResponseType {
        ResponseType::Blob
    }
    fn request_state_mut(request: &mut SingleBlockLookup<T>) -> &mut Self {
        &mut request.blob_request_state
    }
    fn get_state(&self) -> &SingleLookupRequestState<Self::VerifiedResponseType> {
        &self.state
    }
    fn get_state_mut(&mut self) -> &mut SingleLookupRequestState<Self::VerifiedResponseType> {
        &mut self.state
    }
}

impl<T: BeaconChainTypes> RequestState<T> for CustodyRequestState<T::EthSpec> {
    type VerifiedResponseType = Vec<CustodyDataColumn<T::EthSpec>>;

    fn make_request(
        &self,
        id: Id,
        // TODO(das): consider selecting peers that have custody but are in this set
        _peer_id: PeerId,
        downloaded_block_expected_blobs: Option<usize>,
        cx: &mut SyncNetworkContext<T>,
    ) -> Result<bool, LookupRequestError> {
        cx.custody_lookup_request(id, self.block_root, downloaded_block_expected_blobs)
            .map_err(LookupRequestError::SendFailed)
    }

    fn send_for_processing(
        id: Id,
        download_result: DownloadResult<Self::VerifiedResponseType>,
        cx: &SyncNetworkContext<T>,
    ) -> Result<(), LookupRequestError> {
        let DownloadResult {
            value,
            block_root,
            seen_timestamp,
            ..
        } = download_result;
        cx.send_custody_columns_for_processing(
            block_root,
            value,
            seen_timestamp,
            BlockProcessType::SingleCustodyColumn(id),
        )
        .map_err(LookupRequestError::SendFailed)
    }

    fn response_type() -> ResponseType {
        ResponseType::CustodyColumn
    }
    fn request_state_mut(request: &mut SingleBlockLookup<T>) -> &mut Self {
        &mut request.custody_request_state
    }
    fn get_state(&self) -> &SingleLookupRequestState<Self::VerifiedResponseType> {
        &self.state
    }
    fn get_state_mut(&mut self) -> &mut SingleLookupRequestState<Self::VerifiedResponseType> {
        &mut self.state
    }
}
