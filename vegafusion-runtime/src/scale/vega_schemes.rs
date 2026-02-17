pub(crate) enum NamedRange {
    Scheme {
        scheme: &'static str,
        extent: Option<(f64, f64)>,
    },
    Values(&'static [&'static str]),
}

pub(crate) enum SchemePalette {
    Discrete(&'static [&'static str]),
    Continuous(&'static str),
}

const SYMBOL_RANGE: &[&str] = &[
    "circle",
    "square",
    "triangle-up",
    "cross",
    "diamond",
    "triangle-right",
    "triangle-down",
    "triangle-left",
];

const TABLEAU10: &[&str] = &[
    "#4c78a8", "#f58518", "#e45756", "#72b7b2", "#54a24b", "#eeca3b", "#b279a2", "#ff9da6",
    "#9d755d", "#bab0ac",
];

const INFERNO: &str = concat!(
    "0000040403130c0826170c3b240c4f330a5f420a68500d6c5d126e6b176e781c6d86",
    "216b932667a12b62ae305cbb3755c73e4cd24644dd513ae65c30ed6925f3771af8850f",
    "fb9506fca50afcb519fac62df6d645f2e661f3f484fcffa4"
);

const MAGMA: &str = concat!(
    "0000040404130b0924150e3720114b2c11603b0f704a107957157e651a80721f817f24",
    "828c29819a2e80a8327db6377ac43c75d1426fde4968e95462f1605df76f5cfa7f5efc",
    "8f65fe9f6dfeaf78febf84fece91fddea0fcedaffcfdbf"
);

const PLASMA: &str = concat!(
    "0d088723069033059742039d5002a25d01a66a00a87801a88405a7900da49c179ea721",
    "98b12a90ba3488c33d80cb4779d35171da5a69e16462e76e5bed7953f2834cf68f44fa",
    "9a3dfca636fdb32ffec029fcce25f9dc24f5ea27f0f921"
);

const VIRIDIS: &str = concat!(
    "440154470e61481a6c482575472f7d443a834144873d4e8a39568c35608d31688e2d708e",
    "2a788e27818e23888e21918d1f988b1fa08822a8842ab07f35b77943bf7154c56866cc5d",
    "7ad1518fd744a5db36bcdf27d2e21be9e51afde725"
);

const CIVIDIS: &str = concat!(
    "00205100235800265d002961012b65042e670831690d346b11366c16396d1c3c6e213f6e",
    "26426e2c456e31476e374a6e3c4d6e42506e47536d4c566d51586e555b6e5a5e6e5e616e",
    "62646f66676f6a6a706e6d717270717573727976737c79747f7c75827f75868276898577",
    "8c8877908b78938e789691789a94789e9778a19b78a59e77a9a177aea575b2a874b6ab73",
    "bbaf71c0b26fc5b66dc9b96acebd68d3c065d8c462ddc85fe2cb5ce7cf58ebd355f0d652",
    "f3da4ff7de4cfae249fce647"
);

const BLUES: &str = "cfe1f2bed8eca8cee58fc1de74b2d75ba3cf4592c63181bd206fb2125ca40a4a90";
const BLUEORANGE: &str = "134b852f78b35da2cb9dcae1d2e5eff2f0ebfce0bafbbf74e8932fc5690d994a07";
const YELLOWGREENBLUE: &str = "eff9bddbf1b4bde5b594d5b969c5be45b4c22c9ec02182b82163aa23479c1c3185";

pub(crate) fn default_named_range(name: &str) -> Option<NamedRange> {
    let lowered = name.to_ascii_lowercase();
    match lowered.as_str() {
        "category" => Some(NamedRange::Scheme {
            scheme: "tableau10",
            extent: None,
        }),
        "ordinal" => Some(NamedRange::Scheme {
            scheme: "blues",
            extent: None,
        }),
        "heatmap" => Some(NamedRange::Scheme {
            scheme: "yellowgreenblue",
            extent: None,
        }),
        "ramp" => Some(NamedRange::Scheme {
            scheme: "blues",
            extent: None,
        }),
        "diverging" => Some(NamedRange::Scheme {
            scheme: "blueorange",
            extent: Some((1.0, 0.0)),
        }),
        "symbol" => Some(NamedRange::Values(SYMBOL_RANGE)),
        _ => None,
    }
}

pub(crate) fn lookup_scheme(name: &str) -> Option<SchemePalette> {
    let lowered = name.to_ascii_lowercase();
    match lowered.as_str() {
        "tableau10" => Some(SchemePalette::Discrete(TABLEAU10)),
        "blues" => Some(SchemePalette::Continuous(BLUES)),
        "blueorange" => Some(SchemePalette::Continuous(BLUEORANGE)),
        "yellowgreenblue" => Some(SchemePalette::Continuous(YELLOWGREENBLUE)),
        "inferno" => Some(SchemePalette::Continuous(INFERNO)),
        "magma" => Some(SchemePalette::Continuous(MAGMA)),
        "plasma" => Some(SchemePalette::Continuous(PLASMA)),
        "viridis" => Some(SchemePalette::Continuous(VIRIDIS)),
        "cividis" => Some(SchemePalette::Continuous(CIVIDIS)),
        _ => None,
    }
}

pub(crate) fn decode_continuous_scheme(hex: &str) -> Vec<String> {
    hex.as_bytes()
        .chunks_exact(6)
        .map(|chunk| {
            let color = std::str::from_utf8(chunk).unwrap_or_default();
            format!("#{color}")
        })
        .collect()
}
