use super::goldilocks_quintic::QuinticElement;
use std::fs::File;
use std::io::Read;
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Scalar(pub [u64; 5]);

const N: Scalar = Scalar([
    0xE80FD996948BFFE1,
    0xE8885C39D724A09C,
    0x7FFFFFE6CFB80639,
    0x7FFFFFF100000016,
    0x7FFFFFFD80000007,
]);
const N0I: u64 = 0xD78BEF72057B7BDF;
const R2: Scalar = Scalar([
    0xA01001DCE33DC739,
    0x6C3228D33F62ACCF,
    0xD1D796CC91CF8525,
    0xAADFFF5D1574C1D8,
    0x4ACA13B28CA251F5,
]);
static RANDOM_SOURCE: OnceLock<Mutex<File>> = OnceLock::new();

pub const ZERO: Scalar = Scalar([0, 0, 0, 0, 0]);
pub const ONE: Scalar = Scalar([1, 0, 0, 0, 0]);

impl Scalar {
    pub fn to_le_bytes(self) -> [u8; 40] {
        let mut out = [0u8; 40];
        for (index, limb) in self.0.iter().enumerate() {
            out[index * 8..(index + 1) * 8].copy_from_slice(&limb.to_le_bytes());
        }
        out
    }

    pub fn from_le_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != 40 {
            return Err("scalar bytes length must be 40");
        }
        let mut limbs = [0u64; 5];
        for (index, limb) in limbs.iter_mut().enumerate() {
            let mut bytes8 = [0u8; 8];
            bytes8.copy_from_slice(&bytes[index * 8..(index + 1) * 8]);
            *limb = u64::from_le_bytes(bytes8);
        }
        Ok(Self(limbs))
    }

    pub fn random() -> Result<Self, &'static str> {
        if RANDOM_SOURCE.get().is_none() {
            let file = File::open("/dev/urandom").map_err(|_| "failed to open /dev/urandom")?;
            let _ = RANDOM_SOURCE.set(Mutex::new(file));
        }
        let random_source = RANDOM_SOURCE.get().ok_or("failed to open /dev/urandom")?;
        let mut file = random_source
            .lock()
            .map_err(|_| "random source lock poisoned")?;
        Self::random_from_reader(&mut *file)
    }

    fn random_from_reader(reader: &mut impl Read) -> Result<Self, &'static str> {
        loop {
            let mut bytes = [0u8; 40];
            reader
                .read_exact(&mut bytes)
                .map_err(|_| "failed to read /dev/urandom")?;
            bytes[39] &= 0x7f;
            let scalar = Self::from_le_bytes(&bytes)?;
            if !scalar.is_zero() && scalar.cmp(&N) == core::cmp::Ordering::Less {
                return Ok(scalar);
            }
        }
    }

    pub fn from_quintic(value: QuinticElement) -> Self {
        let mut scalar = Self(value.canonical_array());
        while scalar.cmp(&N) != core::cmp::Ordering::Less {
            scalar = scalar.sub_inner(&N).0;
        }
        scalar
    }

    pub fn add_inner(self, rhs: Self) -> Self {
        let mut out = [0u64; 5];
        let mut carry = 0u64;
        for (index, limb) in out.iter_mut().enumerate() {
            let (sum1, carry1) = self.0[index].overflowing_add(rhs.0[index]);
            let (sum2, carry2) = sum1.overflowing_add(carry);
            *limb = sum2;
            carry = carry1 as u64 + carry2 as u64;
        }
        Self(out)
    }

    pub fn sub_inner(self, rhs: &Self) -> (Self, u64) {
        let mut out = [0u64; 5];
        let mut borrow = 0u64;
        for (index, limb) in out.iter_mut().enumerate() {
            let (diff1, borrow1) = self.0[index].overflowing_sub(rhs.0[index]);
            let (diff2, borrow2) = diff1.overflowing_sub(borrow);
            *limb = diff2;
            borrow = (borrow1 as u64) | (borrow2 as u64);
        }
        let mask = if borrow != 0 { u64::MAX } else { 0 };
        (Self(out), mask)
    }

    pub fn add(self, rhs: Self) -> Self {
        let r0 = self.add_inner(rhs);
        let (r1, borrow) = r0.sub_inner(&N);
        select(borrow, r1, r0)
    }

    pub fn sub(self, rhs: Self) -> Self {
        let (r0, borrow) = self.sub_inner(&rhs);
        let r1 = r0.add_inner(N);
        select(borrow, r0, r1)
    }

    pub fn neg(self) -> Self {
        ZERO.sub(self)
    }

    pub fn monty_mul(self, rhs: &Self) -> Self {
        let mut r = [0u64; 5];
        for i in 0..5 {
            let m = rhs.0[i];
            let f = self.0[0]
                .wrapping_mul(m)
                .wrapping_add(r[0])
                .wrapping_mul(N0I);

            let mut carry1 = 0u64;
            let mut carry2 = 0u64;
            for j in 0..5 {
                let z1 = U128::from_u64(self.0[j])
                    .mul_u64(m)
                    .add_u64(r[j])
                    .add_u64(carry1);
                carry1 = z1.hi;
                let z2 = U128::from_u64(f)
                    .mul_u64(N.0[j])
                    .add_u64(z1.lo)
                    .add_u64(carry2);
                carry2 = z2.hi;
                if j > 0 {
                    r[j - 1] = z2.lo;
                }
            }
            r[4] = carry1.wrapping_add(carry2);
        }
        let reduced = Self(r);
        let (r2, borrow) = reduced.sub_inner(&N);
        select(borrow, r2, reduced)
    }

    pub fn mul(self, rhs: &Self) -> Self {
        self.monty_mul(&R2).monty_mul(rhs)
    }

    pub fn square(self) -> Self {
        self.mul(&self)
    }

    pub fn recode_signed(self, output: &mut [i32], width: i32) {
        recode_signed_from_limbs(&self.0, output, width);
    }

    pub fn is_zero(self) -> bool {
        self.0.iter().all(|limb| *limb == 0)
    }

    fn cmp(&self, rhs: &Self) -> core::cmp::Ordering {
        for index in (0..5).rev() {
            match self.0[index].cmp(&rhs.0[index]) {
                core::cmp::Ordering::Equal => {}
                ordering => return ordering,
            }
        }
        core::cmp::Ordering::Equal
    }
}

#[derive(Clone, Copy)]
struct U128 {
    hi: u64,
    lo: u64,
}

impl U128 {
    fn from_u64(value: u64) -> Self {
        Self { hi: 0, lo: value }
    }

    fn add_u64(self, value: u64) -> Self {
        let (lo, carry) = self.lo.overflowing_add(value);
        Self {
            hi: self.hi.wrapping_add(carry as u64),
            lo,
        }
    }

    fn sub_u64(self, value: u64) -> Self {
        let (lo, borrow) = self.lo.overflowing_sub(value);
        Self {
            hi: self.hi.wrapping_sub(borrow as u64),
            lo,
        }
    }

    fn mul_u64(self, value: u64) -> Self {
        let product = self.lo as u128 * value as u128;
        Self {
            hi: ((product >> 64) as u64).wrapping_add(self.hi.wrapping_mul(value)),
            lo: product as u64,
        }
    }
}

fn select(mask: u64, a0: Scalar, a1: Scalar) -> Scalar {
    let mut out = [0u64; 5];
    for (index, limb) in out.iter_mut().enumerate() {
        *limb = a0.0[index] ^ (mask & (a0.0[index] ^ a1.0[index]));
    }
    Scalar(out)
}

pub fn recode_signed_from_limbs(limbs: &[u64], output: &mut [i32], width: i32) {
    let mut acc = 0u64;
    let mut acc_len = 0i32;
    let mut limb_index = 0usize;
    let mask_width = (1u32 << width) - 1;
    let half_width = 1u32 << (width - 1);
    let mut carry = 0u32;

    for digit in output.iter_mut() {
        let mut chunk: u32;
        if acc_len < width {
            if limb_index < limbs.len() {
                let next_limb = limbs[limb_index];
                limb_index += 1;
                chunk = ((acc | next_limb.wrapping_shl(acc_len as u32)) as u32) & mask_width;
                acc = next_limb >> (width - acc_len);
            } else {
                chunk = (acc as u32) & mask_width;
                acc = 0;
            }
            acc_len += 64 - width;
        } else {
            chunk = (acc as u32) & mask_width;
            acc_len -= width;
            acc >>= width;
        }

        chunk = chunk.wrapping_add(carry);
        carry = (half_width.wrapping_sub(chunk)) >> 31;
        *digit = chunk as i32 - ((carry << width) as i32);
    }
}

#[cfg(test)]
mod tests {
    use super::super::goldilocks::GoldilocksField;
    use super::*;
    use std::io::{self, Read};

    struct CountingReader {
        reads: usize,
        payloads: Vec<[u8; 40]>,
    }

    impl Read for CountingReader {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let payload = self.payloads.remove(0);
            out[..payload.len()].copy_from_slice(&payload);
            self.reads += 1;
            Ok(payload.len())
        }
    }

    #[test]
    fn serdes_match_go_vector() {
        let scalar = Scalar([
            6950590877883398434,
            17178336263794770543,
            11012823478139181320,
            16445091359523510936,
            5882925226143600273,
        ]);
        assert_eq!(
            Scalar::from_le_bytes(&scalar.to_le_bytes()).unwrap(),
            scalar
        );
    }

    #[test]
    fn random_from_reader_reuses_reader_without_reopening_source() {
        let mut payload = [0u8; 40];
        payload[0] = 7;
        let mut reader = CountingReader {
            reads: 0,
            payloads: vec![payload, payload],
        };

        let first = Scalar::random_from_reader(&mut reader).unwrap();
        let second = Scalar::random_from_reader(&mut reader).unwrap();

        assert_eq!(first, second);
        assert_eq!(reader.reads, 2);
    }

    #[test]
    fn add_sub_inner_match_go_vectors() {
        let scalar1 = Scalar([u64::MAX; 5]);
        let scalar2 = Scalar([
            0xFFFFFFFFFeeFFF,
            12312321312,
            0xFFFFFFFFFFFFFFFF,
            0xFFFFFFacdFFFFF,
            0xbcaFFFFFFFFFFFFF,
        ]);
        assert_eq!(
            scalar1.add_inner(scalar2),
            Scalar([
                0xfffffffffeeffe,
                0x2dddf1d20,
                0xffffffffffffffff,
                0xffffffacdfffff,
                0xbcafffffffffffff,
            ])
        );

        let (result, borrow) = ZERO.sub_inner(&Scalar([u64::MAX; 5]));
        assert_eq!(result, Scalar([1, 0, 0, 0, 0]));
        assert_eq!(borrow, u64::MAX);
    }

    #[test]
    fn sub_monty_mul_and_mul_match_go_vectors() {
        let result = Scalar([1, 2, 0, 0, 0]).sub(Scalar([u64::MAX; 5]));
        assert_eq!(
            result,
            Scalar([
                0xe80fd996948bffe3,
                0xe8885c39d724a09e,
                0x7fffffe6cfb80639,
                0x7ffffff100000016,
                0x7ffffffd80000007,
            ])
        );

        let monty = Scalar([1, 2, 3, 4, 5]).monty_mul(&Scalar([u64::MAX; 5]));
        assert_eq!(
            monty,
            Scalar([
                10974894505036100890,
                7458803775930281466,
                744239893213209819,
                3396127080529349464,
                5979369289905897562,
            ])
        );

        let squared = Scalar([u64::MAX; 5]).mul(&Scalar([u64::MAX; 5]));
        assert_eq!(
            squared,
            Scalar([
                471447996674510360,
                3520142298321118626,
                17240611161823899731,
                5610669884293437850,
                1193611606749909414,
            ])
        );
    }

    #[test]
    fn recode_signed_matches_go_vector() {
        let mut output = [0i32; 50];
        let scalar = Scalar([
            super::super::goldilocks::ORDER - 1,
            super::super::goldilocks::ORDER - 2,
            super::super::goldilocks::ORDER - 3,
            u64::MAX,
            super::super::goldilocks::ORDER - 5,
        ]);
        scalar.recode_signed(&mut output, 5);
        for (index, value) in output.iter().enumerate() {
            let expected = match index {
                6 => -4,
                19 => -2,
                25 => -8,
                32 => -1,
                _ => 0,
            };
            assert_eq!(*value, expected, "index {index}");
        }
    }

    #[test]
    fn from_quintic_matches_go_vector() {
        let neg_one = GoldilocksField::neg_one();
        let scalar = Scalar::from_quintic(QuinticElement([
            neg_one, neg_one, neg_one, neg_one, neg_one,
        ]));
        assert_eq!(
            scalar,
            Scalar([
                3449841778703204414,
                3382000508875488967,
                212073444237,
                124554051540,
                17179869170,
            ])
        );
    }
}
