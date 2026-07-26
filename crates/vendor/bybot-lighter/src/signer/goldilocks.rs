pub const ORDER: u64 = 0xffffffff00000001;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GoldilocksField(pub u64);

impl GoldilocksField {
    pub fn zero() -> Self {
        Self(0)
    }

    pub fn one() -> Self {
        Self(1)
    }

    pub fn neg_one() -> Self {
        Self(ORDER - 1)
    }

    pub fn from_u64(value: u64) -> Self {
        Self((value as u128 % ORDER as u128) as u64)
    }

    pub fn from_u32(value: u32) -> Self {
        Self::from_u64(value as u64)
    }

    pub fn from_i64(value: i64) -> Self {
        if value >= 0 {
            Self::from_u64(value as u64)
        } else {
            Self::from_u64(value.unsigned_abs()).neg()
        }
    }

    pub fn canonical(self) -> u64 {
        if self.0 >= ORDER {
            self.0 - ORDER
        } else {
            self.0
        }
    }

    pub fn is_zero(self) -> bool {
        self.canonical() == 0
    }

    pub fn add(self, rhs: Self) -> Self {
        Self::from_u64(
            ((self.canonical() as u128 + rhs.canonical() as u128) % ORDER as u128) as u64,
        )
    }

    pub fn sub(self, rhs: Self) -> Self {
        let lhs = self.canonical() as u128;
        let rhs = rhs.canonical() as u128;
        Self::from_u64(((lhs + ORDER as u128 - rhs) % ORDER as u128) as u64)
    }

    pub fn mul(self, rhs: Self) -> Self {
        Self::from_u64(
            ((self.canonical() as u128 * rhs.canonical() as u128) % ORDER as u128) as u64,
        )
    }

    pub fn square(self) -> Self {
        self.mul(self)
    }

    pub fn pow_u64(self, exponent: u64) -> Self {
        let mut result = Self::one();
        let mut base = self;
        let mut exp = exponent;
        while exp > 0 {
            if exp & 1 == 1 {
                result = result.mul(base);
            }
            base = base.square();
            exp >>= 1;
        }
        result
    }

    pub fn inverse_or_zero(self) -> Self {
        if self.is_zero() {
            Self::zero()
        } else {
            self.pow_u64(ORDER - 2)
        }
    }

    pub fn neg(self) -> Self {
        if self.is_zero() {
            Self::zero()
        } else {
            Self(ORDER - self.canonical())
        }
    }

    pub fn to_le_bytes(self) -> [u8; 8] {
        self.canonical().to_le_bytes()
    }

    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self::from_u64(u64::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INPUTS: &[u64] = &[
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        2147483638,
        2147483639,
        2147483640,
        2147483641,
        2147483642,
        2147483643,
        2147483644,
        2147483645,
        2147483646,
        2147483647,
        2147483648,
        2147483649,
        2147483650,
        2147483651,
        2147483652,
        2147483653,
        2147483654,
        2147483655,
        2147483656,
        2147483657,
        4294967286,
        4294967287,
        4294967288,
        4294967289,
        4294967290,
        4294967291,
        4294967292,
        4294967293,
        4294967294,
        4294967295,
        4294967296,
        4294967297,
        4294967298,
        4294967299,
        4294967300,
        4294967301,
        4294967302,
        4294967303,
        4294967304,
        4294967305,
        9223372036854775798,
        9223372036854775799,
        9223372036854775800,
        9223372036854775801,
        9223372036854775802,
        9223372036854775803,
        9223372036854775804,
        9223372036854775805,
        9223372036854775806,
        9223372036854775807,
        9223372036854775808,
        9223372036854775809,
        9223372036854775810,
        9223372036854775811,
        9223372036854775812,
        9223372036854775813,
        9223372036854775814,
        9223372036854775815,
        9223372036854775816,
        9223372036854775817,
        18446744069414584311,
        18446744069414584312,
        18446744069414584313,
        18446744069414584314,
        18446744069414584315,
        18446744069414584316,
        18446744069414584317,
        18446744069414584318,
        18446744069414584319,
        18446744069414584320,
    ];

    #[test]
    fn add_sub_mul_neg_square_match_mod_arithmetic() {
        for &lhs in INPUTS {
            for &rhs in INPUTS {
                let l = GoldilocksField::from_u64(lhs);
                let r = GoldilocksField::from_u64(rhs);
                let order = ORDER as u128;
                assert_eq!(
                    l.add(r).canonical(),
                    ((lhs as u128 + rhs as u128) % order) as u64
                );
                assert_eq!(
                    l.sub(r).canonical(),
                    (((lhs as u128 % order) + order - (rhs as u128 % order)) % order) as u64
                );
                assert_eq!(
                    l.mul(r).canonical(),
                    ((lhs as u128 * rhs as u128) % order) as u64
                );
            }
            let l = GoldilocksField::from_u64(lhs);
            let order = ORDER as u128;
            assert_eq!(
                l.neg().canonical(),
                ((order - (lhs as u128 % order)) % order) as u64
            );
            assert_eq!(
                l.square().canonical(),
                ((lhs as u128 * lhs as u128) % order) as u64
            );
        }
    }

    #[test]
    fn wraparound_cases_match_go_tests() {
        let a = GoldilocksField::from_u64((ORDER + 1) / 2);
        let b = GoldilocksField::from_u64(2);
        let x = a.mul(b);
        assert_eq!(x.canonical(), GoldilocksField::one().canonical());
        assert_eq!(
            GoldilocksField::zero().sub(x).canonical(),
            GoldilocksField::neg_one().canonical()
        );

        let a = GoldilocksField::from_u64(u64::MAX - ORDER);
        let b = GoldilocksField::neg_one();
        let c = a.add(a).add(b.add(b));
        let d = a.add(b).add(a.add(b));
        assert_eq!(c.canonical(), d.canonical());
    }

    #[test]
    fn little_endian_round_trip() {
        for &value in INPUTS {
            let f = GoldilocksField::from_u64(value);
            let bytes = f.to_le_bytes();
            assert_eq!(
                GoldilocksField::from_le_bytes(bytes).canonical(),
                f.canonical()
            );
        }
    }
}
