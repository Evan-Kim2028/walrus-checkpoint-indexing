//! DeepBook event decoding helpers.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sui_types::base_types::{ObjectID, SuiAddress};

/// Decode DeepBook event BCS into a JSON string. Returns a structured error JSON
/// if the event type is unknown or decode fails.
pub fn event_json_from_bcs(event_type: &str, bcs_data: &[u8]) -> Result<String> {
    if let Some(decoded) = decode_event_json(event_type, bcs_data) {
        return Ok(decoded);
    }

    let value = json!({
        "event_type": event_type,
        "decode_error": "unsupported_event_type_or_decode_failure",
    });
    serde_json::to_string(&value).context("failed to serialize event_json")
}

/// Decode DeepBook event BCS into a JSON string. Returns None on unknown type
/// or decode failure.
pub fn decode_event_json(event_type: &str, bcs_data: &[u8]) -> Option<String> {
    match event_type {
        "OrderPlaced" => decode_deepbook_event("OrderPlaced", bcs_data, |ev: MoveOrderPlacedEvent| {
            json!({
                "event_type": "OrderPlaced",
                "fields": {
                    "balance_manager_id": ev.balance_manager_id.to_string(),
                    "pool_id": ev.pool_id.to_string(),
                    "order_id": ev.order_id.to_string(),
                    "client_order_id": ev.client_order_id,
                    "trader": ev.trader.to_string(),
                    "price": ev.price,
                    "is_bid": ev.is_bid,
                    "placed_quantity": ev.placed_quantity,
                    "expire_timestamp": ev.expire_timestamp,
                    "timestamp": ev.timestamp,
                }
            })
        }),
        "OrderModified" => decode_deepbook_event("OrderModified", bcs_data, |ev: MoveOrderModifiedEvent| {
            json!({
                "event_type": "OrderModified",
                "fields": {
                    "balance_manager_id": ev.balance_manager_id.to_string(),
                    "pool_id": ev.pool_id.to_string(),
                    "order_id": ev.order_id.to_string(),
                    "client_order_id": ev.client_order_id,
                    "trader": ev.trader.to_string(),
                    "price": ev.price,
                    "is_bid": ev.is_bid,
                    "previous_quantity": ev.previous_quantity,
                    "filled_quantity": ev.filled_quantity,
                    "new_quantity": ev.new_quantity,
                    "timestamp": ev.timestamp,
                }
            })
        }),
        "OrderCanceled" => decode_deepbook_event("OrderCanceled", bcs_data, |ev: MoveOrderCanceledEvent| {
            json!({
                "event_type": "OrderCanceled",
                "fields": {
                    "balance_manager_id": ev.balance_manager_id.to_string(),
                    "pool_id": ev.pool_id.to_string(),
                    "order_id": ev.order_id.to_string(),
                    "client_order_id": ev.client_order_id,
                    "trader": ev.trader.to_string(),
                    "price": ev.price,
                    "is_bid": ev.is_bid,
                    "original_quantity": ev.original_quantity,
                    "base_asset_quantity_canceled": ev.base_asset_quantity_canceled,
                    "timestamp": ev.timestamp,
                }
            })
        }),
        "OrderExpired" => decode_deepbook_event("OrderExpired", bcs_data, |ev: MoveOrderExpiredEvent| {
            json!({
                "event_type": "OrderExpired",
                "fields": {
                    "balance_manager_id": ev.balance_manager_id.to_string(),
                    "pool_id": ev.pool_id.to_string(),
                    "order_id": ev.order_id.to_string(),
                    "client_order_id": ev.client_order_id,
                    "trader": ev.trader.to_string(),
                    "price": ev.price,
                    "is_bid": ev.is_bid,
                    "original_quantity": ev.original_quantity,
                    "base_asset_quantity_canceled": ev.base_asset_quantity_canceled,
                    "timestamp": ev.timestamp,
                }
            })
        }),
        "OrderFilled" => decode_deepbook_event("OrderFilled", bcs_data, |ev: MoveOrderFilledEvent| {
            json!({
                "event_type": "OrderFilled",
                "fields": {
                    "pool_id": ev.pool_id.to_string(),
                    "maker_order_id": ev.maker_order_id.to_string(),
                    "taker_order_id": ev.taker_order_id.to_string(),
                    "maker_client_order_id": ev.maker_client_order_id,
                    "taker_client_order_id": ev.taker_client_order_id,
                    "price": ev.price,
                    "taker_is_bid": ev.taker_is_bid,
                    "taker_fee": ev.taker_fee,
                    "taker_fee_is_deep": ev.taker_fee_is_deep,
                    "maker_fee": ev.maker_fee,
                    "maker_fee_is_deep": ev.maker_fee_is_deep,
                    "base_quantity": ev.base_quantity,
                    "quote_quantity": ev.quote_quantity,
                    "maker_balance_manager_id": ev.maker_balance_manager_id.to_string(),
                    "taker_balance_manager_id": ev.taker_balance_manager_id.to_string(),
                    "timestamp": ev.timestamp,
                }
            })
        }),
        "FlashLoanBorrowed" => {
            decode_deepbook_event("FlashLoanBorrowed", bcs_data, |ev: MoveFlashLoanBorrowedEvent| {
                json!({
                    "event_type": "FlashLoanBorrowed",
                    "fields": {
                        "pool_id": ev.pool_id.to_string(),
                        "borrow_quantity": ev.borrow_quantity,
                        "type_name": ev.type_name,
                    }
                })
            })
        }
        "PriceAdded" => decode_deepbook_event("PriceAdded", bcs_data, |ev: MovePriceAddedEvent| {
            json!({
                "event_type": "PriceAdded",
                "fields": {
                    "conversion_rate": ev.conversion_rate,
                    "timestamp": ev.timestamp,
                    "is_base_conversion": ev.is_base_conversion,
                    "reference_pool": ev.reference_pool.to_string(),
                    "target_pool": ev.target_pool.to_string(),
                }
            })
        }),
        "BalanceEvent" => decode_deepbook_event("BalanceEvent", bcs_data, |ev: MoveBalanceEvent| {
            json!({
                "event_type": "BalanceEvent",
                "fields": {
                    "balance_manager_id": ev.balance_manager_id.to_string(),
                    "asset": ev.asset,
                    "amount": ev.amount,
                    "deposit": ev.deposit,
                }
            })
        }),
        "ProposalEvent" => decode_deepbook_event("ProposalEvent", bcs_data, |ev: MoveProposalEvent| {
            json!({
                "event_type": "ProposalEvent",
                "fields": {
                    "pool_id": ev.pool_id.to_string(),
                    "balance_manager_id": ev.balance_manager_id.to_string(),
                    "epoch": ev.epoch,
                    "taker_fee": ev.taker_fee,
                    "maker_fee": ev.maker_fee,
                    "stake_required": ev.stake_required,
                }
            })
        }),
        "RebateEvent" => decode_deepbook_event("RebateEvent", bcs_data, |ev: MoveRebateEvent| {
            json!({
                "event_type": "RebateEvent",
                "fields": {
                    "pool_id": ev.pool_id.to_string(),
                    "balance_manager_id": ev.balance_manager_id.to_string(),
                    "epoch": ev.epoch,
                    "claim_amount": ev.claim_amount,
                }
            })
        }),
        "StakeEvent" => decode_deepbook_event("StakeEvent", bcs_data, |ev: MoveStakeEvent| {
            json!({
                "event_type": "StakeEvent",
                "fields": {
                    "pool_id": ev.pool_id.to_string(),
                    "balance_manager_id": ev.balance_manager_id.to_string(),
                    "epoch": ev.epoch,
                    "amount": ev.amount,
                    "stake": ev.stake,
                }
            })
        }),
        "TradeParamsUpdateEvent" => {
            decode_deepbook_event(
                "TradeParamsUpdateEvent",
                bcs_data,
                |ev: MoveTradeParamsUpdateEvent| {
                    json!({
                        "event_type": "TradeParamsUpdateEvent",
                        "fields": {
                            "taker_fee": ev.taker_fee,
                            "maker_fee": ev.maker_fee,
                            "stake_required": ev.stake_required,
                        }
                    })
                },
            )
        }
        "VoteEvent" => decode_deepbook_event("VoteEvent", bcs_data, |ev: MoveVoteEvent| {
            json!({
                "event_type": "VoteEvent",
                "fields": {
                    "pool_id": ev.pool_id.to_string(),
                    "balance_manager_id": ev.balance_manager_id.to_string(),
                    "epoch": ev.epoch,
                    "from_proposal_id": ev.from_proposal_id.map(|id| id.to_string()),
                    "to_proposal_id": ev.to_proposal_id.to_string(),
                    "stake": ev.stake,
                }
            })
        }),
        _ => None,
    }
}

fn decode_deepbook_event<T, F>(event_type: &str, bcs_data: &[u8], builder: F) -> Option<String>
where
    T: serde::de::DeserializeOwned,
    F: FnOnce(T) -> serde_json::Value,
{
    let decoded: T = match bcs::from_bytes(bcs_data) {
        Ok(decoded) => decoded,
        Err(err) => {
            tracing::debug!(event_type, error = %err, "failed to decode deepbook event");
            return None;
        }
    };

    let value = builder(decoded);
    serde_json::to_string(&value).ok()
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveOrderFilledEvent {
    pub pool_id: ObjectID,
    pub maker_order_id: u128,
    pub taker_order_id: u128,
    pub maker_client_order_id: u64,
    pub taker_client_order_id: u64,
    pub price: u64,
    pub taker_is_bid: bool,
    pub taker_fee: u64,
    pub taker_fee_is_deep: bool,
    pub maker_fee: u64,
    pub maker_fee_is_deep: bool,
    pub base_quantity: u64,
    pub quote_quantity: u64,
    pub maker_balance_manager_id: ObjectID,
    pub taker_balance_manager_id: ObjectID,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveOrderCanceledEvent {
    pub balance_manager_id: ObjectID,
    pub pool_id: ObjectID,
    pub order_id: u128,
    pub client_order_id: u64,
    pub trader: SuiAddress,
    pub price: u64,
    pub is_bid: bool,
    pub original_quantity: u64,
    pub base_asset_quantity_canceled: u64,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveOrderExpiredEvent {
    pub balance_manager_id: ObjectID,
    pub pool_id: ObjectID,
    pub order_id: u128,
    pub client_order_id: u64,
    pub trader: SuiAddress,
    pub price: u64,
    pub is_bid: bool,
    pub original_quantity: u64,
    pub base_asset_quantity_canceled: u64,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveOrderModifiedEvent {
    pub balance_manager_id: ObjectID,
    pub pool_id: ObjectID,
    pub order_id: u128,
    pub client_order_id: u64,
    pub trader: SuiAddress,
    pub price: u64,
    pub is_bid: bool,
    pub previous_quantity: u64,
    pub filled_quantity: u64,
    pub new_quantity: u64,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveOrderPlacedEvent {
    pub balance_manager_id: ObjectID,
    pub pool_id: ObjectID,
    pub order_id: u128,
    pub client_order_id: u64,
    pub trader: SuiAddress,
    pub price: u64,
    pub is_bid: bool,
    pub placed_quantity: u64,
    pub expire_timestamp: u64,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MovePriceAddedEvent {
    pub conversion_rate: u64,
    pub timestamp: u64,
    pub is_base_conversion: bool,
    pub reference_pool: ObjectID,
    pub target_pool: ObjectID,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveFlashLoanBorrowedEvent {
    pub pool_id: ObjectID,
    pub borrow_quantity: u64,
    pub type_name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveBalanceEvent {
    pub balance_manager_id: ObjectID,
    pub asset: String,
    pub amount: u64,
    pub deposit: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveTradeParamsUpdateEvent {
    pub taker_fee: u64,
    pub maker_fee: u64,
    pub stake_required: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveStakeEvent {
    pub pool_id: ObjectID,
    pub balance_manager_id: ObjectID,
    pub epoch: u64,
    pub amount: u64,
    pub stake: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveProposalEvent {
    pub pool_id: ObjectID,
    pub balance_manager_id: ObjectID,
    pub epoch: u64,
    pub taker_fee: u64,
    pub maker_fee: u64,
    pub stake_required: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveVoteEvent {
    pub pool_id: ObjectID,
    pub balance_manager_id: ObjectID,
    pub epoch: u64,
    pub from_proposal_id: Option<ObjectID>,
    pub to_proposal_id: ObjectID,
    pub stake: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
struct MoveRebateEvent {
    pub pool_id: ObjectID,
    pub balance_manager_id: ObjectID,
    pub epoch: u64,
    pub claim_amount: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn order_placed_json_shape() {
        let pool_id = ObjectID::from_hex_literal(
            "0x0000000000000000000000000000000000000000000000000000000000000001",
        )
        .unwrap();
        let balance_manager_id = ObjectID::from_hex_literal(
            "0x0000000000000000000000000000000000000000000000000000000000000002",
        )
        .unwrap();
        let trader = SuiAddress::from_str(
            "0x0000000000000000000000000000000000000000000000000000000000000003",
        )
        .unwrap();

        let event = MoveOrderPlacedEvent {
            balance_manager_id,
            pool_id,
            order_id: 123u128,
            client_order_id: 42,
            trader,
            price: 100,
            is_bid: true,
            placed_quantity: 5,
            expire_timestamp: 999,
            timestamp: 123456,
        };

        let bcs_data = bcs::to_bytes(&event).unwrap();
        let json_str = decode_event_json("OrderPlaced", &bcs_data).expect("decode should succeed");
        let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(value["event_type"], "OrderPlaced");
        assert_eq!(value["fields"]["order_id"], "123");
        assert_eq!(value["fields"]["client_order_id"], 42);
        assert_eq!(value["fields"]["is_bid"], true);
        assert_eq!(
            value["fields"]["pool_id"],
            "0x0000000000000000000000000000000000000000000000000000000000000001"
        );
        assert_eq!(
            value["fields"]["balance_manager_id"],
            "0x0000000000000000000000000000000000000000000000000000000000000002"
        );
        assert_eq!(
            value["fields"]["trader"],
            "0x0000000000000000000000000000000000000000000000000000000000000003"
        );
    }
}
