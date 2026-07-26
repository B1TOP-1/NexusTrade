use super::curve::{mul_generator, GENERATOR};
use super::goldilocks::GoldilocksField;
use super::goldilocks_quintic::QuinticElement;
use super::poseidon2::hash_to_quintic_extension;
use super::scalar::Scalar;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Signature {
    pub s: Scalar,
    pub e: Scalar,
}

impl Signature {
    pub fn to_bytes(self) -> [u8; 80] {
        let mut out = [0u8; 80];
        out[..40].copy_from_slice(&self.s.to_le_bytes());
        out[40..].copy_from_slice(&self.e.to_le_bytes());
        out
    }
}

pub fn public_key_from_secret(secret_key: Scalar) -> QuinticElement {
    GENERATOR.mul(&secret_key).encode()
}

pub fn sign_hashed_message_with_k(
    hashed_message: QuinticElement,
    secret_key: Scalar,
    k: Scalar,
) -> Signature {
    let r = mul_generator(&k).encode();
    let mut preimage = [GoldilocksField::zero(); 10];
    preimage[..5].copy_from_slice(&r.0);
    preimage[5..].copy_from_slice(&hashed_message.0);

    let e = Scalar::from_quintic(hash_to_quintic_extension(&preimage));
    Signature {
        s: k.sub(e.mul(&secret_key)),
        e,
    }
}

pub fn sign_hashed_message(
    hashed_message: QuinticElement,
    secret_key: Scalar,
) -> Result<Signature, &'static str> {
    let k = Scalar::random()?;
    Ok(sign_hashed_message_with_k(hashed_message, secret_key, k))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(values: [u64; 5]) -> QuinticElement {
        QuinticElement::from_u64_array(values)
    }

    #[test]
    fn sign_with_fixed_k_matches_go_vectors() {
        let secret_keys = [
            Scalar([
                12235002942052073545,
                1175977464658719998,
                8536934969147463310,
                6524687619313720391,
                2922072024880609112,
            ]),
            Scalar([
                14609471659974493146,
                15558617123161593410,
                853367204868339037,
                17594253198278631904,
                368396584122947478,
            ]),
            Scalar([
                846395111423676945,
                1354180063821346280,
                5751371120309175011,
                4898038106472090654,
                1076345918732914302,
            ]),
        ];
        let hashed_messages = [
            q([
                8398652514106806347,
                11069112711939986896,
                9732488227085561369,
                18076754337204438535,
                17155407358725346236,
            ]),
            q([
                14569490467507212064,
                2707063505563578676,
                7506743487465742335,
                12569771346154554175,
                4305083698940175790,
            ]),
            q([
                17529153479246803593,
                1743712677205511695,
                4834285972617397460,
                5486672566342530358,
                7254989001695704129,
            ]),
        ];
        let ks = [
            Scalar([
                5245666847777449560,
                15178169970799106939,
                4403065012435293749,
                15306540389399388999,
                8935555081913173844,
            ]),
            Scalar([
                1980123857560067020,
                10696795398834097509,
                3211831869376171671,
                6194822139276031840,
                3482023782412490864,
            ]),
            Scalar([
                10299597990997564957,
                8547298489021408803,
                12250978550108858722,
                5282281975236198197,
                5328603554431393061,
            ]),
        ];
        let expected_s = [
            Scalar([
                6950590877883398434,
                17178336263794770543,
                11012823478139181320,
                16445091359523510936,
                5882925226143600273,
            ]),
            Scalar([
                15189311883262425203,
                16924634885527914505,
                11098200095411565797,
                11441434601417451505,
                2245797172600273048,
            ]),
            Scalar([
                1747989245728027396,
                18083435619737379521,
                18276259610811995786,
                15101757397705334408,
                5007814817019340642,
            ]),
        ];
        let expected_e = [
            Scalar([
                4544744459434870309,
                4180764085957612004,
                3024669018778978615,
                15433417688859446606,
                6775027260348937828,
            ]),
            Scalar([
                4905460437060282008,
                9275377852059362729,
                10383772785796962929,
                6858067464918579610,
                7078247668913970626,
            ]),
            Scalar([
                4911725746357568132,
                12205663641120664338,
                16433506899074513700,
                14763562571101437023,
                2547950465160283358,
            ]),
        ];

        for index in 0..secret_keys.len() {
            let signature =
                sign_hashed_message_with_k(hashed_messages[index], secret_keys[index], ks[index]);
            assert_eq!(signature.s, expected_s[index], "S index {index}");
            assert_eq!(signature.e, expected_e[index], "E index {index}");
        }
    }
}
