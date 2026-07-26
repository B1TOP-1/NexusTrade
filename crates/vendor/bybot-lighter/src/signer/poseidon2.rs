use super::goldilocks::GoldilocksField;
use super::goldilocks_quintic::QuinticElement;

const WIDTH: usize = 12;
const RATE: usize = 8;
const ROUNDS_F_HALF: usize = 4;
const ROUNDS_P: usize = 22;

const EXTERNAL_CONSTANTS: [[GoldilocksField; WIDTH]; 8] = [
    [
        GoldilocksField(15492826721047263190),
        GoldilocksField(11728330187201910315),
        GoldilocksField(8836021247773420868),
        GoldilocksField(16777404051263952451),
        GoldilocksField(5510875212538051896),
        GoldilocksField(6173089941271892285),
        GoldilocksField(2927757366422211339),
        GoldilocksField(10340958981325008808),
        GoldilocksField(8541987352684552425),
        GoldilocksField(9739599543776434497),
        GoldilocksField(15073950188101532019),
        GoldilocksField(12084856431752384512),
    ],
    [
        GoldilocksField(4584713381960671270),
        GoldilocksField(8807052963476652830),
        GoldilocksField(54136601502601741),
        GoldilocksField(4872702333905478703),
        GoldilocksField(5551030319979516287),
        GoldilocksField(12889366755535460989),
        GoldilocksField(16329242193178844328),
        GoldilocksField(412018088475211848),
        GoldilocksField(10505784623379650541),
        GoldilocksField(9758812378619434837),
        GoldilocksField(7421979329386275117),
        GoldilocksField(375240370024755551),
    ],
    [
        GoldilocksField(3331431125640721931),
        GoldilocksField(15684937309956309981),
        GoldilocksField(578521833432107983),
        GoldilocksField(14379242000670861838),
        GoldilocksField(17922409828154900976),
        GoldilocksField(8153494278429192257),
        GoldilocksField(15904673920630731971),
        GoldilocksField(11217863998460634216),
        GoldilocksField(3301540195510742136),
        GoldilocksField(9937973023749922003),
        GoldilocksField(3059102938155026419),
        GoldilocksField(1895288289490976132),
    ],
    [
        GoldilocksField(5580912693628927540),
        GoldilocksField(10064804080494788323),
        GoldilocksField(9582481583369602410),
        GoldilocksField(10186259561546797986),
        GoldilocksField(247426333829703916),
        GoldilocksField(13193193905461376067),
        GoldilocksField(6386232593701758044),
        GoldilocksField(17954717245501896472),
        GoldilocksField(1531720443376282699),
        GoldilocksField(2455761864255501970),
        GoldilocksField(11234429217864304495),
        GoldilocksField(4746959618548874102),
    ],
    [
        GoldilocksField(13571697342473846203),
        GoldilocksField(17477857865056504753),
        GoldilocksField(15963032953523553760),
        GoldilocksField(16033593225279635898),
        GoldilocksField(14252634232868282405),
        GoldilocksField(8219748254835277737),
        GoldilocksField(7459165569491914711),
        GoldilocksField(15855939513193752003),
        GoldilocksField(16788866461340278896),
        GoldilocksField(7102224659693946577),
        GoldilocksField(3024718005636976471),
        GoldilocksField(13695468978618890430),
    ],
    [
        GoldilocksField(8214202050877825436),
        GoldilocksField(2670727992739346204),
        GoldilocksField(16259532062589659211),
        GoldilocksField(11869922396257088411),
        GoldilocksField(3179482916972760137),
        GoldilocksField(13525476046633427808),
        GoldilocksField(3217337278042947412),
        GoldilocksField(14494689598654046340),
        GoldilocksField(15837379330312175383),
        GoldilocksField(8029037639801151344),
        GoldilocksField(2153456285263517937),
        GoldilocksField(8301106462311849241),
    ],
    [
        GoldilocksField(13294194396455217955),
        GoldilocksField(17394768489610594315),
        GoldilocksField(12847609130464867455),
        GoldilocksField(14015739446356528640),
        GoldilocksField(5879251655839607853),
        GoldilocksField(9747000124977436185),
        GoldilocksField(8950393546890284269),
        GoldilocksField(10765765936405694368),
        GoldilocksField(14695323910334139959),
        GoldilocksField(16366254691123000864),
        GoldilocksField(15292774414889043182),
        GoldilocksField(10910394433429313384),
    ],
    [
        GoldilocksField(17253424460214596184),
        GoldilocksField(3442854447664030446),
        GoldilocksField(3005570425335613727),
        GoldilocksField(10859158614900201063),
        GoldilocksField(9763230642109343539),
        GoldilocksField(6647722546511515039),
        GoldilocksField(909012944955815706),
        GoldilocksField(18101204076790399111),
        GoldilocksField(11588128829349125809),
        GoldilocksField(15863878496612806566),
        GoldilocksField(5201119062417750399),
        GoldilocksField(176665553780565743),
    ],
];

const INTERNAL_CONSTANTS: [GoldilocksField; ROUNDS_P] = [
    GoldilocksField(11921381764981422944),
    GoldilocksField(10318423381711320787),
    GoldilocksField(8291411502347000766),
    GoldilocksField(229948027109387563),
    GoldilocksField(9152521390190983261),
    GoldilocksField(7129306032690285515),
    GoldilocksField(15395989607365232011),
    GoldilocksField(8641397269074305925),
    GoldilocksField(17256848792241043600),
    GoldilocksField(6046475228902245682),
    GoldilocksField(12041608676381094092),
    GoldilocksField(12785542378683951657),
    GoldilocksField(14546032085337914034),
    GoldilocksField(3304199118235116851),
    GoldilocksField(16499627707072547655),
    GoldilocksField(10386478025625759321),
    GoldilocksField(13475579315436919170),
    GoldilocksField(16042710511297532028),
    GoldilocksField(1411266850385657080),
    GoldilocksField(9024840976168649958),
    GoldilocksField(14047056970978379368),
    GoldilocksField(838728605080212101),
];

const MATRIX_DIAG_12: [GoldilocksField; WIDTH] = [
    GoldilocksField(0xc3b6c08e23ba9300),
    GoldilocksField(0xd84b5de94a324fb6),
    GoldilocksField(0x0d0c371c5b35b84f),
    GoldilocksField(0x7964f570e7188037),
    GoldilocksField(0x5daf18bbd996604b),
    GoldilocksField(0x6743bc47b9595257),
    GoldilocksField(0x5528b9362c59bb70),
    GoldilocksField(0xac45e25b7127b68b),
    GoldilocksField(0xa2077d7dfbb606b5),
    GoldilocksField(0xf3faac6faee378ae),
    GoldilocksField(0x0c6388b51545e883),
    GoldilocksField(0xd27dbb6944917b60),
];

pub fn hash_to_quintic_extension(input: &[GoldilocksField]) -> QuinticElement {
    let output = hash_n_to_m_no_pad(input, 5);
    QuinticElement([output[0], output[1], output[2], output[3], output[4]])
}

pub fn hash_n_to_hash_no_pad(input: &[GoldilocksField]) -> [GoldilocksField; 4] {
    let output = hash_n_to_m_no_pad(input, 4);
    [output[0], output[1], output[2], output[3]]
}

pub fn hash_n_to_m_no_pad(input: &[GoldilocksField], output_count: usize) -> Vec<GoldilocksField> {
    let mut state = [GoldilocksField::zero(); WIDTH];
    let mut offset = 0;
    while offset < input.len() {
        let remaining = input.len() - offset;
        let chunk_len = remaining.min(RATE);
        state[..chunk_len].copy_from_slice(&input[offset..offset + chunk_len]);
        permute(&mut state);
        offset += RATE;
    }

    let mut outputs = Vec::with_capacity(output_count);
    loop {
        for value in state.iter().take(RATE) {
            outputs.push(*value);
            if outputs.len() == output_count {
                return outputs;
            }
        }
        permute(&mut state);
    }
}

pub fn permute(state: &mut [GoldilocksField; WIDTH]) {
    external_linear_layer(state);
    full_rounds(state, 0);
    partial_rounds(state);
    full_rounds(state, ROUNDS_F_HALF);
}

fn full_rounds(state: &mut [GoldilocksField; WIDTH], start: usize) {
    for round in start..start + ROUNDS_F_HALF {
        add_external_round_constants(state, round);
        sbox(state);
        external_linear_layer(state);
    }
}

fn partial_rounds(state: &mut [GoldilocksField; WIDTH]) {
    for (round, constant) in INTERNAL_CONSTANTS.iter().enumerate() {
        let _ = round;
        state[0] = state[0].add(*constant);
        sbox_at(state, 0);
        internal_linear_layer(state);
    }
}

fn external_linear_layer(state: &mut [GoldilocksField; WIDTH]) {
    for window in 0..3 {
        let offset = 4 * window;
        let t0 = state[offset].add(state[offset + 1]);
        let t1 = state[offset + 2].add(state[offset + 3]);
        let t2 = t0.add(t1);
        let t3 = t2.add(state[offset + 1]);
        let t4 = t2.add(state[offset + 3]);
        let t5 = state[offset].add(state[offset]);
        let t6 = state[offset + 2].add(state[offset + 2]);

        state[offset] = t3.add(t0);
        state[offset + 1] = t6.add(t3);
        state[offset + 2] = t1.add(t4);
        state[offset + 3] = t5.add(t4);
    }

    let mut sums = [GoldilocksField::zero(); 4];
    for k in 0..4 {
        for j in (0..WIDTH).step_by(4) {
            sums[k] = sums[k].add(state[j + k]);
        }
    }
    for i in 0..WIDTH {
        state[i] = state[i].add(sums[i % 4]);
    }
}

fn internal_linear_layer(state: &mut [GoldilocksField; WIDTH]) {
    let mut sum = state[0];
    for value in state.iter().skip(1) {
        sum = sum.add(*value);
    }
    for (index, value) in state.iter_mut().enumerate() {
        *value = value.mul(MATRIX_DIAG_12[index]).add(sum);
    }
}

fn add_external_round_constants(state: &mut [GoldilocksField; WIDTH], round: usize) {
    for (index, value) in state.iter_mut().enumerate() {
        *value = value.add(EXTERNAL_CONSTANTS[round][index]);
    }
}

fn sbox(state: &mut [GoldilocksField; WIDTH]) {
    for index in 0..WIDTH {
        sbox_at(state, index);
    }
}

fn sbox_at(state: &mut [GoldilocksField; WIDTH], index: usize) {
    let value = state[index];
    let value_square = value.square();
    let value_sixth = value_square.mul(value).square();
    state[index] = value_sixth.mul(value);
}

#[cfg(test)]
mod tests {
    use super::super::goldilocks::ORDER;
    use super::*;

    #[test]
    fn permute_matches_go_vector() {
        let mut input = [
            GoldilocksField(5417613058500526590),
            GoldilocksField(2481548824842427254),
            GoldilocksField(6473243198879784792),
            GoldilocksField(1720313757066167274),
            GoldilocksField(2806320291675974571),
            GoldilocksField(7407976414706455446),
            GoldilocksField(1105257841424046885),
            GoldilocksField(7613435757403328049),
            GoldilocksField(3376066686066811538),
            GoldilocksField(5888575799323675710),
            GoldilocksField(6689309723188675948),
            GoldilocksField(2468250420241012720),
        ];
        permute(&mut input);
        assert_eq!(
            input.map(GoldilocksField::canonical),
            [
                5364184781011389007,
                15309475861242939136,
                5983386513087443499,
                886942118604446276,
                14903657885227062600,
                7742650891575941298,
                1962182278500985790,
                10213480816595178755,
                3510799061817443836,
                4610029967627506430,
                7566382334276534836,
                2288460879362380348,
            ]
        );
    }

    #[test]
    fn hash_n_to_m_no_pad_matches_go_vector() {
        let input = [
            GoldilocksField(2963773914414780088),
            GoldilocksField(8389525300242074234),
            GoldilocksField(3700959901615818008),
            GoldilocksField(6116199383751757212),
            GoldilocksField(3418607418699599889),
            GoldilocksField(8793277256263635044),
            GoldilocksField(448623437464918480),
            GoldilocksField(1857310021116627925),
            GoldilocksField(6145634616307237342),
            GoldilocksField(1548353948794474539),
            GoldilocksField(2318110128254703527),
            GoldilocksField(8347759953730634762),
        ];
        let result = hash_n_to_m_no_pad(&input, 12);
        let result: Vec<u64> = result.into_iter().map(GoldilocksField::canonical).collect();
        assert_eq!(
            result,
            vec![
                3627923032009111551,
                1460752551327577353,
                1084214837491058067,
                1841622875286057462,
                3996252440506437984,
                1276718204392552803,
                8564515621134952155,
                9252927025993202701,
                1147435538714642916,
                16407277821156164797,
                11997661877740155273,
                12485021000320141292,
            ]
        );
    }

    #[test]
    fn hash_n_to_hash_no_pad_matches_go_vectors() {
        let result = hash_n_to_hash_no_pad(&[
            GoldilocksField(11295517158488612626),
            GoldilocksField(10669470463693797151),
            GoldilocksField(17232114065640264171),
            GoldilocksField(4175927072186299193),
            GoldilocksField(13985285184240204531),
            GoldilocksField(7901017084268693144),
            GoldilocksField(4326299618263946178),
            GoldilocksField(14787024750292535041),
            GoldilocksField(894520636503353046),
            GoldilocksField(12556655399058578835),
            GoldilocksField(3097737892474696200),
            GoldilocksField(7515335668060050861),
        ]);
        assert_eq!(
            result.map(GoldilocksField::canonical),
            [
                15396602476382546759,
                12422280135166335470,
                8165681190607828974,
                3475588160239961712,
            ]
        );

        let large_result = hash_n_to_hash_no_pad(&[
            GoldilocksField(ORDER + 1),
            GoldilocksField(ORDER + 2),
            GoldilocksField(ORDER + 3),
            GoldilocksField(u64::MAX),
            GoldilocksField(u64::MAX - 1),
        ]);
        assert_eq!(
            large_result.map(GoldilocksField::canonical),
            [
                14216040864787980138,
                17275303675000904868,
                11831395338463193314,
                281267649235863375,
            ]
        );
    }

    #[test]
    fn hash_to_quintic_extension_matches_go_vector() {
        let result = hash_to_quintic_extension(&[
            GoldilocksField(3451004116618606032),
            GoldilocksField(11263134342958518251),
            GoldilocksField(10957204882857370932),
            GoldilocksField(5369763041201481933),
            GoldilocksField(7695734348563036858),
            GoldilocksField(1393419330378128434),
            GoldilocksField(7387917082382606332),
        ]);
        assert_eq!(
            result.canonical_array(),
            [
                17992684813643984528,
                5243896189906434327,
                7705560276311184368,
                2785244775876017560,
                14449776097783372302,
            ]
        );
    }
}
