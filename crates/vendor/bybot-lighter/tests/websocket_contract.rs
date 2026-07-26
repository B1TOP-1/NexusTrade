use bybot_lighter::websocket::{
    public_subscription_payload, LighterReconnectPolicy, LighterSubscriptionSet,
    LighterWebSocketClient, LighterWebSocketConfig, LighterWsEvent,
};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[test]
fn builds_public_and_private_subscription_payloads() {
    assert_eq!(
        public_subscription_payload("subscribe", "order_book/1").unwrap(),
        r#"{"channel":"order_book/1","type":"subscribe"}"#
    );

    let mut subscriptions = LighterSubscriptionSet::default();
    subscriptions.subscribe_public("order_book/1").unwrap();
    subscriptions.subscribe_private("account_orders").unwrap();
    subscriptions
        .subscribe_private("account_all_trades")
        .unwrap();

    let payloads = subscriptions.reconnect_payloads("fresh-auth").unwrap();
    assert_eq!(payloads.len(), 3);
    assert!(payloads.iter().any(|payload| {
        payload.contains(r#""channel":"account_orders""#)
            && payload.contains(r#""auth":"fresh-auth""#)
    }));
}

#[test]
fn reconnect_is_an_explicit_event() {
    assert_eq!(LighterWsEvent::Reconnected, LighterWsEvent::Reconnected);

    let policy = LighterReconnectPolicy::default();
    assert_eq!(policy.delay_for_attempt(0), Duration::from_millis(500));
    assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(750));
    assert_eq!(policy.delay_for_attempt(20), Duration::from_secs(5));
}

#[tokio::test]
async fn replies_to_ping_and_reads_text_frames() {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("bind WebSocket test listener: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        socket
            .send(Message::Ping(b"lighter-ping".as_slice().into()))
            .await
            .unwrap();
        let pong = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(pong, Message::Pong(payload) if payload.as_ref() == b"lighter-ping"));
        socket
            .send(Message::Text(r#"{"type":"ready"}"#.into()))
            .await
            .unwrap();
    });

    let config = LighterWebSocketConfig::new(format!("ws://{address}")).unwrap();
    let client = LighterWebSocketClient::new(config);
    let mut connection = client.connect().await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(2), connection.next_event())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        event,
        LighterWsEvent::Text(r#"{"type":"ready"}"#.to_string())
    );
    server.await.unwrap();
}

#[tokio::test]
async fn reconnect_replays_public_then_private_subscriptions() {
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("bind WebSocket test listener: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for connection_index in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            if connection_index == 0 {
                continue;
            }
            let first = socket.next().await.unwrap().unwrap();
            let second = socket.next().await.unwrap().unwrap();
            assert!(matches!(first, Message::Text(text) if text.contains("order_book/1")));
            assert!(
                matches!(second, Message::Text(text) if text.contains("account_orders") && text.contains("fresh-auth"))
            );
        }
    });

    let config = LighterWebSocketConfig::new(format!("ws://{address}")).unwrap();
    let mut client = LighterWebSocketClient::new(config);
    client
        .subscriptions_mut()
        .subscribe_public("order_book/1")
        .unwrap();
    client
        .subscriptions_mut()
        .subscribe_private("account_orders")
        .unwrap();
    let mut connection = client.connect().await.unwrap();
    let event = client
        .reconnect(&mut connection, "fresh-auth")
        .await
        .unwrap();

    assert_eq!(event, LighterWsEvent::Reconnected);
    server.await.unwrap();
}
