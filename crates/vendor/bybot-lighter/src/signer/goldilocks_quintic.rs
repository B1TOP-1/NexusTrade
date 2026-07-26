use super::goldilocks::GoldilocksField;

const FP5_D: usize = 5;
const FP5_W: GoldilocksField = GoldilocksField(3);
const FP5_DTH_ROOT: GoldilocksField = GoldilocksField(1041288259238279555);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QuinticElement(pub [GoldilocksField; FP5_D]);

impl QuinticElement {
    pub fn zero() -> Self {
        Self([GoldilocksField::zero(); FP5_D])
    }

    pub fn one() -> Self {
        Self::from_field(GoldilocksField::one())
    }

    pub fn from_field(value: GoldilocksField) -> Self {
        Self([
            value,
            GoldilocksField::zero(),
            GoldilocksField::zero(),
            GoldilocksField::zero(),
            GoldilocksField::zero(),
        ])
    }

    pub fn from_u64_array(values: [u64; FP5_D]) -> Self {
        Self(values.map(GoldilocksField::from_u64))
    }

    pub fn canonical_array(self) -> [u64; FP5_D] {
        self.0.map(GoldilocksField::canonical)
    }

    pub fn to_le_bytes(self) -> [u8; 40] {
        let mut out = [0u8; 40];
        for (index, limb) in self.0.iter().enumerate() {
            out[index * 8..(index + 1) * 8].copy_from_slice(&limb.to_le_bytes());
        }
        out
    }

    pub fn from_le_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != 40 {
            return Err("quintic bytes length must be 40");
        }
        let mut limbs = [GoldilocksField::zero(); FP5_D];
        for (index, limb) in limbs.iter_mut().enumerate() {
            let mut bytes8 = [0u8; 8];
            bytes8.copy_from_slice(&bytes[index * 8..(index + 1) * 8]);
            *limb = GoldilocksField::from_le_bytes(bytes8);
        }
        Ok(Self(limbs))
    }

    pub fn is_zero(self) -> bool {
        self.0.iter().all(|limb| limb.is_zero())
    }

    pub fn neg(self) -> Self {
        Self(self.0.map(GoldilocksField::neg))
    }

    pub fn add(self, rhs: Self) -> Self {
        let mut out = [GoldilocksField::zero(); FP5_D];
        for (index, limb) in out.iter_mut().enumerate() {
            *limb = self.0[index].add(rhs.0[index]);
        }
        Self(out)
    }

    pub fn sub(self, rhs: Self) -> Self {
        let mut out = [GoldilocksField::zero(); FP5_D];
        for (index, limb) in out.iter_mut().enumerate() {
            *limb = self.0[index].sub(rhs.0[index]);
        }
        Self(out)
    }

    pub fn mul(self, rhs: Self) -> Self {
        let w = FP5_W;

        let a0b0 = self.0[0].mul(rhs.0[0]);
        let a1b4 = self.0[1].mul(rhs.0[4]);
        let a2b3 = self.0[2].mul(rhs.0[3]);
        let a3b2 = self.0[3].mul(rhs.0[2]);
        let a4b1 = self.0[4].mul(rhs.0[1]);
        let c0 = add_many([a0b0, w.mul(add_many([a1b4, a2b3, a3b2, a4b1]))]);

        let a0b1 = self.0[0].mul(rhs.0[1]);
        let a1b0 = self.0[1].mul(rhs.0[0]);
        let a2b4 = self.0[2].mul(rhs.0[4]);
        let a3b3 = self.0[3].mul(rhs.0[3]);
        let a4b2 = self.0[4].mul(rhs.0[2]);
        let c1 = add_many([a0b1, a1b0, w.mul(add_many([a2b4, a3b3, a4b2]))]);

        let a0b2 = self.0[0].mul(rhs.0[2]);
        let a1b1 = self.0[1].mul(rhs.0[1]);
        let a2b0 = self.0[2].mul(rhs.0[0]);
        let a3b4 = self.0[3].mul(rhs.0[4]);
        let a4b3 = self.0[4].mul(rhs.0[3]);
        let c2 = add_many([a0b2, a1b1, a2b0, w.mul(add_many([a3b4, a4b3]))]);

        let a0b3 = self.0[0].mul(rhs.0[3]);
        let a1b2 = self.0[1].mul(rhs.0[2]);
        let a2b1 = self.0[2].mul(rhs.0[1]);
        let a3b0 = self.0[3].mul(rhs.0[0]);
        let a4b4 = self.0[4].mul(rhs.0[4]);
        let c3 = add_many([a0b3, a1b2, a2b1, a3b0, w.mul(a4b4)]);

        let a0b4 = self.0[0].mul(rhs.0[4]);
        let a1b3 = self.0[1].mul(rhs.0[3]);
        let a2b2 = self.0[2].mul(rhs.0[2]);
        let a3b1 = self.0[3].mul(rhs.0[1]);
        let a4b0 = self.0[4].mul(rhs.0[0]);
        let c4 = add_many([a0b4, a1b3, a2b2, a3b1, a4b0]);

        Self([c0, c1, c2, c3, c4])
    }

    pub fn square(self) -> Self {
        let w = FP5_W;
        let double_w = w.add(w);

        let a0s = self.0[0].square();
        let a1a4 = self.0[1].mul(self.0[4]);
        let a2a3 = self.0[2].mul(self.0[3]);
        let c0 = a0s.add(double_w.mul(a1a4.add(a2a3)));

        let a0_double = self.0[0].add(self.0[0]);
        let a0_double_a1 = a0_double.mul(self.0[1]);
        let a2a4_double_w = self.0[2].mul(self.0[4]).mul(double_w);
        let a3a3w = self.0[3].square().mul(w);
        let c1 = add_many([a0_double_a1, a2a4_double_w, a3a3w]);

        let a0_double_a2 = a0_double.mul(self.0[2]);
        let a1_square = self.0[1].square();
        let a4a3_double_w = self.0[4].mul(self.0[3]).mul(double_w);
        let c2 = add_many([a0_double_a2, a1_square, a4a3_double_w]);

        let a1_double = self.0[1].add(self.0[1]);
        let a0_double_a3 = a0_double.mul(self.0[3]);
        let a1_double_a2 = a1_double.mul(self.0[2]);
        let a4_square_w = self.0[4].square().mul(w);
        let c3 = add_many([a0_double_a3, a1_double_a2, a4_square_w]);

        let a0_double_a4 = a0_double.mul(self.0[4]);
        let a1_double_a3 = a1_double.mul(self.0[3]);
        let a2_square = self.0[2].square();
        let c4 = add_many([a0_double_a4, a1_double_a3, a2_square]);

        Self([c0, c1, c2, c3, c4])
    }

    pub fn scalar_mul(self, scalar: GoldilocksField) -> Self {
        Self(self.0.map(|limb| limb.mul(scalar)))
    }

    pub fn div(self, rhs: Self) -> Self {
        let rhs_inv = rhs.inverse_or_zero();
        if rhs_inv.is_zero() {
            panic!("division by zero");
        }
        self.mul(rhs_inv)
    }

    pub fn double(self) -> Self {
        self.add(self)
    }

    pub fn repeated_frobenius(self, count: usize) -> Self {
        if count == 0 {
            return self;
        }
        let reduced = count % FP5_D;
        if reduced == 0 {
            return self;
        }

        let mut z0 = FP5_DTH_ROOT;
        for _ in 1..reduced {
            z0 = FP5_DTH_ROOT.mul(z0);
        }

        let mut out = [GoldilocksField::zero(); FP5_D];
        let mut power = GoldilocksField::one();
        for (index, limb) in out.iter_mut().enumerate() {
            if index > 0 {
                power = power.mul(z0);
            }
            *limb = self.0[index].mul(power);
        }
        Self(out)
    }

    pub fn frobenius(self) -> Self {
        self.repeated_frobenius(1)
    }

    pub fn inverse_or_zero(self) -> Self {
        if self.is_zero() {
            return Self::zero();
        }

        let d = self.frobenius();
        let e = d.mul(d.frobenius());
        let f = e.mul(e.repeated_frobenius(2));

        let a0b0 = self.0[0].mul(f.0[0]);
        let a1b4 = self.0[1].mul(f.0[4]);
        let a2b3 = self.0[2].mul(f.0[3]);
        let a3b2 = self.0[3].mul(f.0[2]);
        let a4b1 = self.0[4].mul(f.0[1]);
        let added = add_many([a1b4, a2b3, a3b2, a4b1]);
        let base = a0b0.add(FP5_W.mul(added));

        f.scalar_mul(base.inverse_or_zero())
    }
}

fn add_many<const N: usize>(values: [GoldilocksField; N]) -> GoldilocksField {
    values
        .into_iter()
        .fold(GoldilocksField::zero(), GoldilocksField::add)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_value() -> QuinticElement {
        QuinticElement::from_u64_array([
            0x1234567890ABCDEF,
            0x0FEDCBA987654321,
            0x1122334455667788,
            0x8877665544332211,
            0xAABBCCDDEEFF0011,
        ])
    }

    #[test]
    fn add_sub_mul_square_match_go_vectors() {
        let val1 = sample_value();
        let val2 = QuinticElement::from_u64_array([u64::MAX; FP5_D]);

        assert_eq!(
            val1.add(val2).canonical_array(),
            [
                1311768471589866989,
                1147797413325783839,
                1234605620731475846,
                9833440832084189711,
                12302652064957136911,
            ]
        );
        assert_eq!(
            val1.sub(val2).canonical_array(),
            [
                1311768462999932401,
                1147797404735849251,
                1234605612141541258,
                9833440823494255123,
                12302652056367202323,
            ]
        );
        assert_eq!(
            val1.mul(val2).canonical_array(),
            [
                12801331769143413385,
                14031114708135177824,
                4192851210753422088,
                14031114723597060086,
                4193451712464626164,
            ]
        );
        assert_eq!(
            val1.square().canonical_array(),
            [
                2711468769317614959,
                15562737284369360677,
                48874032493986270,
                11211402278708723253,
                2864528669572451733,
            ]
        );
    }

    #[test]
    fn square_matches_mul_self() {
        let values = [
            sample_value(),
            QuinticElement::from_u64_array([0, 1, 2, 3, 4]),
            QuinticElement::from_u64_array([u64::MAX, u64::MAX - 1, 7, 11, 13]),
        ];
        for value in values {
            assert_eq!(value.square(), value.mul(value));
        }
    }

    #[test]
    fn repeated_frobenius_matches_go_vector() {
        let result = sample_value().repeated_frobenius(1);
        assert_eq!(
            result.canonical_array(),
            [
                1311768467294899695,
                5234265561494296110,
                6204816484784411482,
                8858034429214283719,
                17855579289599571296,
            ]
        );
        assert_eq!(sample_value().repeated_frobenius(5), sample_value());
    }

    #[test]
    fn inverse_or_zero_matches_go_vector() {
        let result = sample_value().inverse_or_zero();
        assert_eq!(
            result.canonical_array(),
            [
                10760985268447604442,
                1770001646280707407,
                826117924202660585,
                45414427571889187,
                8256636258983026155,
            ]
        );
        assert_eq!(result.mul(sample_value()), QuinticElement::one());
        assert_eq!(
            QuinticElement::zero().inverse_or_zero(),
            QuinticElement::zero()
        );
    }
}
