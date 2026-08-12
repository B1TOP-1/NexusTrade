use std::str::FromStr;

use nautilus_core::UnixNanos;
use nautilus_core::correctness::CorrectnessError;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Price, Quantity},
};

use crate::{common::consts::GATE_VENUE, http::models::GateFuturesContract};

pub fn parse_gate_futures_contract(
    contract: &GateFuturesContract,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    if contract.status != "trading" {
        anyhow::bail!(
            "Gate futures contract {} is not trading (status: {})",
            contract.name,
            contract.status
        );
    }
    if contract.order_size_min == 0 {
        anyhow::bail!(
            "Gate futures contract {} has invalid order_size_min=0",
            contract.name
        );
    }

    let (base, quote) = contract
        .name
        .split_once('_')
        .ok_or_else(|| anyhow::anyhow!("invalid Gate futures contract name: {}", contract.name))?;
    let quote_currency = Currency::from_str(quote)?;
    let price_increment = Price::from(contract.order_price_round.as_str());
    let size_increment = Quantity::from(1);
    let multiplier = Quantity::from(contract.quanto_multiplier.as_str());
    let min_quantity = Some(Quantity::from(contract.order_size_min.to_string()));
    let max_quantity = contract
        .order_size_max
        .map(|quantity| Quantity::from(quantity.to_string()));

    Ok(InstrumentAny::CryptoPerpetual(
        CryptoPerpetual::new_checked(
            InstrumentId::new(Symbol::new(contract.name.as_str()), *GATE_VENUE),
            Symbol::new(contract.name.as_str()),
            Currency::from_str(base)?,
            quote_currency,
            quote_currency,
            false,
            price_increment.precision,
            size_increment.precision,
            price_increment,
            size_increment,
            Some(multiplier),
            Some(size_increment),
            max_quantity,
            min_quantity,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            ts_event,
            ts_init,
        )
        .map_err(|e: CorrectnessError| anyhow::anyhow!("{e}"))?,
    ))
}

pub fn gate_contract_quantity_to_base(contract: &GateFuturesContract, quantity: Quantity) -> f64 {
    quantity.as_f64() * contract.quanto_multiplier.parse::<f64>().unwrap_or(1.0)
}
