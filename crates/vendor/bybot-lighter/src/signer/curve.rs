use super::goldilocks::GoldilocksField;
use super::goldilocks_quintic::QuinticElement;
use super::scalar::Scalar;
use std::sync::OnceLock;

const WINDOW: usize = 5;
const WIN_SIZE: usize = 1 << (WINDOW - 1);

static GENERATOR_WINDOW: OnceLock<[AffinePoint; WIN_SIZE]> = OnceLock::new();

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Point {
    pub x: QuinticElement,
    pub z: QuinticElement,
    pub u: QuinticElement,
    pub t: QuinticElement,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AffinePoint {
    pub x: QuinticElement,
    pub u: QuinticElement,
}

pub const NEUTRAL: Point = Point {
    x: QuinticElement([GoldilocksField(0); 5]),
    z: QuinticElement([
        GoldilocksField(1),
        GoldilocksField(0),
        GoldilocksField(0),
        GoldilocksField(0),
        GoldilocksField(0),
    ]),
    u: QuinticElement([GoldilocksField(0); 5]),
    t: QuinticElement([
        GoldilocksField(1),
        GoldilocksField(0),
        GoldilocksField(0),
        GoldilocksField(0),
        GoldilocksField(0),
    ]),
};

pub const GENERATOR: Point = Point {
    x: QuinticElement([
        GoldilocksField(12883135586176881569),
        GoldilocksField(4356519642755055268),
        GoldilocksField(5248930565894896907),
        GoldilocksField(2165973894480315022),
        GoldilocksField(2448410071095648785),
    ]),
    z: QuinticElement([
        GoldilocksField(1),
        GoldilocksField(0),
        GoldilocksField(0),
        GoldilocksField(0),
        GoldilocksField(0),
    ]),
    u: QuinticElement([
        GoldilocksField(1),
        GoldilocksField(0),
        GoldilocksField(0),
        GoldilocksField(0),
        GoldilocksField(0),
    ]),
    t: QuinticElement([
        GoldilocksField(4),
        GoldilocksField(0),
        GoldilocksField(0),
        GoldilocksField(0),
        GoldilocksField(0),
    ]),
};

impl Point {
    pub fn equals(self, rhs: Self) -> bool {
        self.u.mul(rhs.t) == rhs.u.mul(self.t)
    }

    pub fn encode(self) -> QuinticElement {
        self.t.mul(self.u.inverse_or_zero())
    }

    pub fn add(self, rhs: Self) -> Self {
        let x1 = self.x;
        let z1 = self.z;
        let u1 = self.u;
        let t1_in = self.t;
        let x2 = rhs.x;
        let z2 = rhs.z;
        let u2 = rhs.u;
        let t2_in = rhs.t;

        let t1 = x1.mul(x2);
        let t2 = z1.mul(z2);
        let t3 = u1.mul(u2);
        let t4 = t1_in.mul(t2_in);
        let t5 = x1.add(z1).mul(x2.add(z2)).sub(t1.add(t2));
        let t6 = u1.add(t1_in).mul(u2.add(t2_in)).sub(t3.add(t4));
        let t7 = t1.add(t2.mul(b()));
        let t8 = t4.mul(t7);
        let t9 = t3.mul(t5.mul(b_mul2()).add(t7.double()));
        let t10 = t4.add(t3.double()).mul(t5.add(t7));

        Self {
            x: t10.sub(t8).mul(b()),
            z: t8.sub(t9),
            u: t6.mul(t2.mul(b()).sub(t1)),
            t: t8.add(t9),
        }
    }

    pub fn double(self) -> Self {
        let mut point = self;
        point.set_double();
        point
    }

    pub fn set_double(&mut self) {
        let x = self.x;
        let z = self.z;
        let u = self.u;
        let t = self.t;

        let t1 = z.mul(t);
        let t2 = t1.mul(t);
        let x1 = t2.square();
        let z1 = t1.mul(u);
        let t3 = u.square();
        let w1 = t2.sub(t3.mul(x.add(z).double()));
        let t4 = z1.square();

        self.x = t4.mul(b_mul4());
        self.z = w1.square();
        self.u = w1.add(z1).square().sub(t4.add(self.z));
        self.t = x1.double().sub(t4.mul(q4()).add(self.z));
    }

    pub fn m_double(self, count: u32) -> Self {
        let mut point = self;
        point.set_m_double(count);
        point
    }

    pub fn set_m_double(&mut self, count: u32) {
        if count == 0 {
            return;
        }
        if count == 1 {
            self.set_double();
            return;
        }

        let x0 = self.x;
        let z0 = self.z;
        let u0 = self.u;
        let t0 = self.t;

        let mut t1 = z0.mul(t0);
        let mut t2 = t1.mul(t0);
        let x1 = t2.square();
        let z1 = t1.mul(u0);
        let mut t3 = u0.square();
        let w1 = t2.sub(x0.add(z0).double().mul(t3));
        let mut t4 = w1.square();
        let mut t5 = z1.square();
        let mut x = t5.square().mul(b_mul16());
        let mut w = x1.double().sub(t5.mul(q4()).add(t4));
        let mut z = w1.add(z1).square().sub(t4.add(t5));

        for _ in 2..count {
            t1 = z.square();
            t2 = t1.square();
            t3 = w.square();
            t4 = t3.square();
            t5 = w.add(z).square().sub(t1.add(t3));
            z = t5.mul(x.add(t1).double().sub(t3));
            x = t2.mul(t4).mul(b_mul16());
            w = t4.add(t2.mul(b_mul4().sub(q4()))).neg();
        }

        t1 = w.square();
        t2 = z.square();
        t3 = w.add(z).square().sub(t1.add(t2));
        let w1_final = t1.sub(x.add(t2).double());

        self.x = t3.square().mul(b());
        self.z = w1_final.square();
        self.u = t3.mul(w1_final);
        self.t = t1.double().mul(t1.sub(t2.double())).sub(self.z);
    }

    pub fn add_affine(self, rhs: AffinePoint) -> Self {
        let x1 = self.x;
        let z1 = self.z;
        let u1 = self.u;
        let t1_in = self.t;
        let x2 = rhs.x;
        let u2 = rhs.u;

        let t1 = x1.mul(x2);
        let t2 = z1;
        let t3 = u1.mul(u2);
        let t4 = t1_in;
        let t5 = x1.add(x2.mul(z1));
        let t6 = u1.add(u2.mul(t1_in));
        let t7 = t1.add(t2.mul(b()));
        let t8 = t4.mul(t7);
        let t9 = t3.mul(t5.mul(b_mul2()).add(t7.double()));
        let t10 = t4.add(t3.double()).mul(t5.add(t7));

        Self {
            x: t10.sub(t8).mul(b()),
            u: t6.mul(t2.mul(b()).sub(t1)),
            z: t8.sub(t9),
            t: t8.add(t9),
        }
    }

    pub fn make_window_affine(self) -> [AffinePoint; WIN_SIZE] {
        let mut points = [NEUTRAL; WIN_SIZE];
        points[0] = self;
        for index in 1..WIN_SIZE {
            points[index] = if index & 1 == 0 {
                points[index - 1].add(self)
            } else {
                points[index >> 1].double()
            };
        }
        batch_to_affine(&points)
    }

    pub fn mul(self, scalar: &Scalar) -> Self {
        let window = self.make_window_affine();
        mul_with_window(&window, scalar)
    }
}

pub fn mul_generator(scalar: &Scalar) -> Point {
    let window = GENERATOR_WINDOW.get_or_init(|| GENERATOR.make_window_affine());
    mul_with_window(window, scalar)
}

fn mul_with_window(window: &[AffinePoint; WIN_SIZE], scalar: &Scalar) -> Point {
    let mut digits = [0i32; (319 + WINDOW) / WINDOW];
    scalar.recode_signed(&mut digits, WINDOW as i32);

    let mut point = lookup_var_time(&window, digits[digits.len() - 1]).to_point();
    for digit in digits[..digits.len() - 1].iter().rev() {
        point.set_m_double(WINDOW as u32);
        point = point.add_affine(lookup(&window, *digit));
    }
    point
}

impl AffinePoint {
    pub fn to_point(self) -> Point {
        Point {
            x: self.x,
            z: QuinticElement::one(),
            u: self.u,
            t: QuinticElement::one(),
        }
    }

    fn neg(self) -> Self {
        Self {
            x: self.x,
            u: self.u.neg(),
        }
    }
}

pub fn batch_to_affine<const N: usize>(source: &[Point; N]) -> [AffinePoint; N] {
    let mut result = [AffinePoint::default(); N];
    if N == 0 {
        return result;
    }
    if N == 1 {
        let point = source[0];
        let inv = point.z.mul(point.t).inverse_or_zero();
        result[0] = AffinePoint {
            x: point.x.mul(point.t).mul(inv),
            u: point.u.mul(point.z).mul(inv),
        };
        return result;
    }

    let mut product = source[0].z.mul(source[0].t);
    for index in 1..N {
        let x = product;
        product = product.mul(source[index].z);
        let u = product;
        product = product.mul(source[index].t);
        result[index] = AffinePoint { x, u };
    }

    product = product.inverse_or_zero();
    for index in (1..N).rev() {
        result[index].u = source[index].u.mul(result[index].u).mul(product);
        product = product.mul(source[index].t);
        result[index].x = source[index].x.mul(result[index].x).mul(product);
        product = product.mul(source[index].z);
    }
    result[0].u = source[0].u.mul(source[0].z).mul(product);
    product = product.mul(source[0].t);
    result[0].x = source[0].x.mul(product);

    result
}

pub fn lookup(window: &[AffinePoint; WIN_SIZE], digit: i32) -> AffinePoint {
    if digit == 0 {
        return AffinePoint::default();
    }
    if digit > 0 {
        window[(digit - 1) as usize]
    } else {
        window[(-digit - 1) as usize].neg()
    }
}

pub fn lookup_var_time(window: &[AffinePoint; WIN_SIZE], digit: i32) -> AffinePoint {
    lookup(window, digit)
}

fn q4() -> QuinticElement {
    QuinticElement::from_u64_array([4, 0, 0, 0, 0])
}

fn b() -> QuinticElement {
    QuinticElement::from_u64_array([0, 263, 0, 0, 0])
}

fn b_mul2() -> QuinticElement {
    QuinticElement::from_u64_array([0, 526, 0, 0, 0])
}

fn b_mul4() -> QuinticElement {
    QuinticElement::from_u64_array([0, 1052, 0, 0, 0])
}

fn b_mul16() -> QuinticElement {
    QuinticElement::from_u64_array([0, 4208, 0, 0, 0])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(values: [u64; 5]) -> QuinticElement {
        QuinticElement::from_u64_array(values)
    }

    fn sample_a() -> Point {
        Point {
            x: q([
                6598630105941849408,
                1859688128646629097,
                17294281002801957241,
                14913942670710662913,
                10914775081841233526,
            ]),
            z: q([
                5768577777379827814,
                1670898087452303151,
                149395834104961848,
                10215820955974196778,
                12220782198555404872,
            ]),
            u: q([
                8222038236695704789,
                7213480445243459136,
                12261234501547702974,
                16991275954331307770,
                13268460265795104226,
            ]),
            t: q([
                13156365331881093743,
                1228071764139434927,
                12765463901361527883,
                708052950516284594,
                2091843551884526165,
            ]),
        }
    }

    fn sample_b() -> Point {
        Point {
            x: q([
                12601734882931894875,
                8567855799503419472,
                10972305351681971938,
                10379631676278166937,
                14389591363895654229,
            ]),
            z: q([
                7813541982583063146,
                5326831614826269688,
                674248499729254112,
                6075985944329658642,
                4509699573536613779,
            ]),
            u: q([
                18059989919748409029,
                4197498098921379230,
                8619952860870967373,
                4771999616217997413,
                18075221430709764120,
            ]),
            t: q([
                14710659590503370792,
                13425914726164358056,
                15027060927285830507,
                17361235517359536873,
                1738580404337116326,
            ]),
        }
    }

    fn expected_add() -> Point {
        Point {
            x: q([
                2091129225269376836,
                9405624996184206232,
                3901502046808513894,
                17705383837126423407,
                9421907235969101682,
            ]),
            z: q([
                5829667370837222420,
                11237187675958101957,
                1807194474973812009,
                15957008761806494676,
                16213732873017933964,
            ]),
            u: q([
                17708743171457526148,
                7256550674326982355,
                4002326258245501339,
                5920160861215573533,
                6620019694807786845,
            ]),
            t: q([
                8994820555257560065,
                3865139429644955984,
                222111198601608498,
                5080186348564946426,
                910404641634132272,
            ]),
        }
    }

    #[test]
    fn encode_matches_go_vector() {
        let point = Point {
            x: q([
                8219099146870311261,
                1751466925979295147,
                7427996218561204331,
                5499363376829590386,
                17146362437196146248,
            ]),
            z: q([
                9697849239028047855,
                5846309906783017685,
                10545493423738651463,
                2054382452661947581,
                7470471124463677860,
            ]),
            u: q([
                2901139745109740356,
                15850005224840060392,
                3464972059371886732,
                15264046134718393739,
                9208307769190416697,
            ]),
            t: q([
                4691886900801030369,
                14793814721360336872,
                14452533794393275351,
                3652664841353278369,
                4894903405053011144,
            ]),
        };
        assert_eq!(
            point.encode().canonical_array(),
            [
                11698180777452980608,
                17225201015770513568,
                2048901991804183462,
                12372738397545947475,
                13773458998102781339,
            ]
        );
    }

    #[test]
    fn add_and_double_match_go_vectors() {
        let added = sample_a().add(sample_b());
        assert_eq!(added, expected_add());

        let doubled = expected_add().double();
        assert_eq!(
            doubled,
            Point {
                x: q([
                    17841786997947248136,
                    6795260826091178564,
                    17040031878202156690,
                    17452087436690889171,
                    3812897545652133031,
                ]),
                z: q([
                    11020726505488657009,
                    1091762938184204841,
                    4410430720558219763,
                    4363379995258938087,
                    13994951776877072532,
                ]),
                u: q([
                    9442293568698796309,
                    11629160327398360345,
                    1740514571594869537,
                    1168842489343203981,
                    5537908027019165338,
                ]),
                t: q([
                    14684689082562511355,
                    9795998745315395469,
                    11643703245601798489,
                    9164627329631566444,
                    14463660178939261073,
                ]),
            }
        );
    }

    #[test]
    fn m_double_matches_go_vector() {
        let doubled = expected_add().m_double(35);
        assert_eq!(
            doubled,
            Point {
                x: q([
                    5913227576680434070,
                    7982325190863789325,
                    996872074809285515,
                    13250982632111464330,
                    12283818425722177845,
                ]),
                z: q([
                    11109298682748378964,
                    10740549672355474144,
                    8575099619865922741,
                    7569981484002838575,
                    8334331076253814622,
                ]),
                u: q([
                    2081907484718321711,
                    2871920152785433924,
                    16079876071712475691,
                    12304725828108396137,
                    5091453661983356959,
                ]),
                t: q([
                    16573251802693900474,
                    18328109793157914401,
                    5893679867263862011,
                    8243272292726266031,
                    9080497760919830159,
                ]),
            }
        );
    }

    #[test]
    fn scalar_mul_matches_go_vector() {
        let point = Point {
            x: q([
                16818074783491816710,
                5830279414330569119,
                3449083115922675783,
                1268145320872323641,
                12614816166275380125,
            ]),
            z: QuinticElement::one(),
            u: QuinticElement::one(),
            t: q([
                7534507442095725921,
                16658460051907528927,
                12417574136563175256,
                2750788641759288856,
                620002843272906439,
            ]),
        };

        let result = point.mul(&Scalar([
            996458928865875995,
            7368213710557165165,
            8553572641065079816,
            15282443801767955752,
            251150557732720826,
        ]));
        let expected = Point {
            x: q([
                16885333682092300432,
                5595343485914691669,
                13188593663496831978,
                10414629856394645794,
                5668658507670629815,
            ]),
            z: QuinticElement::one(),
            u: QuinticElement::one(),
            t: q([
                9486104512504676657,
                14312981644741144668,
                5159846406177847664,
                15978863787033795628,
                3249948839313771192,
            ]),
        };
        assert!(result.equals(expected));
    }
}
