use bybot_lighter::{
    scaling::{scale_base_amount, scale_price, NonceManager},
    signer::{LighterSigner, TX_TYPE_L2_CANCEL_ORDER, TX_TYPE_L2_CREATE_ORDER},
};
use rust_decimal_macros::dec;

const TEST_PRIVATE_KEY: &str =
    "0xc89d22df8df76acee9f31bd35bdc15afde6324378e760ba8d4feaa233c6292318ad4849dc4285a50";

#[test]
fn signer_scaling_and_nonce_are_nautilus_free() {
    let signer = LighterSigner::new(TEST_PRIVATE_KEY, 304, 1, 42).unwrap();
    let nonce = NonceManager::new();
    nonce.reset(5);

    let create = signer
        .sign_create_order(
            1,
            100,
            scale_base_amount(dec!(0.001), 10_000).unwrap(),
            scale_price(dec!(50_000), 10).unwrap(),
            0,
            0,
            1,
            0,
            0,
            -1,
            nonce.take(),
        )
        .unwrap();
    let cancel = signer.sign_cancel_order(1, 100, nonce.take()).unwrap();

    assert_eq!(create.tx_type, TX_TYPE_L2_CREATE_ORDER);
    assert_eq!(cancel.tx_type, TX_TYPE_L2_CANCEL_ORDER);
    assert!(create.tx_info.contains("\"Nonce\":5"));
    assert!(cancel.tx_info.contains("\"Nonce\":6"));
    assert!(!format!("{signer:?}").contains(TEST_PRIVATE_KEY));
}
