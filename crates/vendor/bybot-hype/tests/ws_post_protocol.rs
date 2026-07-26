use bybot_hype::ws_post::{WsPostRequest, WsPostResponse};
use serde_json::json;

#[test]
fn action_request_matches_official_ws_post_shape() {
    let request = WsPostRequest::action(
        256,
        json!({
            "action": {"type": "order"},
            "nonce": 1713825891591_u64,
            "signature": {"r": "0x1", "s": "0x2", "v": 27},
            "vaultAddress": null
        }),
    );

    let value = request.into_value();
    assert_eq!(value["method"], "post");
    assert_eq!(value["id"], 256);
    assert_eq!(value["request"]["type"], "action");
    assert_eq!(value["request"]["payload"]["nonce"], 1713825891591_u64);
}

#[test]
fn post_response_preserves_request_id_and_payload() {
    let response = WsPostResponse::from_value(json!({
        "channel": "post",
        "data": {
            "id": 256,
            "response": {
                "type": "action",
                "payload": {
                    "status": "ok",
                    "response": {"type": "order", "data": {"statuses": []}}
                }
            }
        }
    }))
    .unwrap();

    assert_eq!(response.request_id(), 256);
    assert_eq!(response.response_type(), "action");
}
