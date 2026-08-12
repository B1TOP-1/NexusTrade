use nautilus_model::identifiers::Venue;
use ustr::Ustr;

pub const GATE: &str = "GATE";
pub const GATE_WS_CHANNEL_FUTURES_OBU: &str = "futures.obu";
pub const GATE_DEFAULT_SETTLE: &str = "usdt";
pub const GATE_DEFAULT_DEPTH: u32 = 50;
pub const GATE_HTTP_PUBLIC_URL: &str = "https://api.gateio.ws/api/v4";
pub const GATE_WS_PUBLIC_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";

/// WebSocket handshake header that makes Gate push size/volume fields as decimal
/// strings instead of integers. Without it, fractional contract sizes (e.g. from
/// partial fills) are floored to int — `0.00015 -> 0`, `1.5 -> 1` — so small fills
/// silently vanish from pushes. Required on the private/execution connection.
pub const GATE_WS_SIZE_DECIMAL_HEADER: (&str, &str) = ("X-Gate-Size-Decimal", "1");

pub static GATE_VENUE: std::sync::LazyLock<Venue> =
    std::sync::LazyLock::new(|| Venue::new(Ustr::from(GATE)));
