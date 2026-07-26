use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPolicy {
    MakerOnly,
    ImmediateOrCancel,
    GoodTillCanceled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderIntent {
    symbol: String,
    asset: usize,
    side: OrderSide,
    limit_price: Decimal,
    size: Decimal,
    reduce_only: bool,
    policy: ExecutionPolicy,
}

impl OrderIntent {
    #[must_use]
    pub fn limit_maker(
        symbol: impl Into<String>,
        asset: usize,
        side: OrderSide,
        limit_price: Decimal,
        size: Decimal,
    ) -> Self {
        Self::new(
            symbol,
            asset,
            side,
            limit_price,
            size,
            false,
            ExecutionPolicy::MakerOnly,
        )
    }

    #[must_use]
    pub fn aggressive_ioc(
        symbol: impl Into<String>,
        asset: usize,
        side: OrderSide,
        limit_price: Decimal,
        size: Decimal,
        reduce_only: bool,
    ) -> Self {
        Self::new(
            symbol,
            asset,
            side,
            limit_price,
            size,
            reduce_only,
            ExecutionPolicy::ImmediateOrCancel,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        symbol: impl Into<String>,
        asset: usize,
        side: OrderSide,
        limit_price: Decimal,
        size: Decimal,
        reduce_only: bool,
        policy: ExecutionPolicy,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            asset,
            side,
            limit_price,
            size,
            reduce_only,
            policy,
        }
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn asset(&self) -> usize {
        self.asset
    }

    #[must_use]
    pub fn side(&self) -> OrderSide {
        self.side
    }

    #[must_use]
    pub fn limit_price(&self) -> Decimal {
        self.limit_price
    }

    #[must_use]
    pub fn size(&self) -> Decimal {
        self.size
    }

    #[must_use]
    pub fn reduce_only(&self) -> bool {
        self.reduce_only
    }

    #[must_use]
    pub fn policy(&self) -> ExecutionPolicy {
        self.policy
    }

    #[must_use]
    pub fn is_maker_only(&self) -> bool {
        self.policy == ExecutionPolicy::MakerOnly
    }

    #[must_use]
    pub fn is_immediate_or_cancel(&self) -> bool {
        self.policy == ExecutionPolicy::ImmediateOrCancel
    }
}
