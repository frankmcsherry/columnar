use bencher::{benchmark_group, benchmark_main, Bencher};
use columnar::{Columnar, Container, Clear, AsBytes, FromBytes};

fn bench_new(b: &mut Bencher) {
    b.iter(|| {
        for _ in 0 .. 1024 {
            Log::new();
        }
    });
}

fn bench_push(b: &mut Bencher) {
    use columnar::Push;
    let log = Log::new();
    let mut container = <Log as Columnar>::Container::default();
    for _ in 0..1024 {
        container.push(&log);
    }
    let mut words = vec![];
    ::columnar::bytes::serialization::encode(&mut words, container.borrow().as_bytes());
    b.bytes = 8 * words.len() as u64;
    b.iter(|| {
        container.clear();
        for _ in 0..1024 {
            container.push(&log);
        }
        bencher::black_box(&container);
    });
}

fn bench_encode(b: &mut Bencher) {
    use columnar::Push;
    let log = Log::new();
    let mut container = <Log as Columnar>::Container::default();
    for _ in 0..1024 {
        container.push(&log);
    }
    let mut words = vec![];
    ::columnar::bytes::serialization::encode(&mut words, container.borrow().as_bytes());
    b.bytes = 8 * words.len() as u64;
    b.iter(|| {
        words.clear();
        ::columnar::bytes::serialization::encode(&mut words, container.borrow().as_bytes());
        bencher::black_box(&words);
    });
}

fn bench_decode(b: &mut Bencher) {
    use columnar::Push;
    let log = Log::new();
    let mut words = vec![];
    let mut container = <Log as Columnar>::Container::default();
    for _ in 0..1024 {
        container.push(&log);
    }
    ::columnar::bytes::serialization::encode(&mut words, container.borrow().as_bytes());
    b.bytes = 8 * words.len() as u64;
    b.iter(|| {
        let mut slices = ::columnar::bytes::serialization::decode(&mut words);
        let foo = <<Log as Columnar>::Container as Container<Log>>::Borrowed::from_bytes(&mut slices);
        bencher::black_box(foo);
    });
}

#[derive(Eq, PartialEq, Columnar)]
pub struct Http {
    protocol: HttpProtocol,
    status: u32,
    host_status: u32,
    up_status: u32,
    method: HttpMethod,
    content_type: String,
    user_agent: String,
    referer: String,
    request_uri: String,
}

#[allow(non_camel_case_types)]
#[derive(Eq, PartialEq, Columnar)]
pub enum HttpProtocol {
    HTTP_PROTOCOL_UNKNOWN,
    HTTP10,
    HTTP11,
}

#[allow(non_camel_case_types)]
#[derive(Eq, PartialEq, Columnar)]
pub enum HttpMethod {
    METHOD_UNKNOWN,
    GET,
    POST,
    DELETE,
    PUT,
    HEAD,
    PURGE,
    OPTIONS,
    PROPFIND,
    MKCOL,
    PATCH,
}

#[allow(non_camel_case_types)]
#[derive(Eq, PartialEq, Columnar)]
pub enum CacheStatus {
    CACHESTATUS_UNKNOWN,
    Miss,
    Expired,
    Hit,
}

#[derive(Eq, PartialEq, Columnar)]
pub struct Origin {
    ip: String,
    port: u32,
    hostname: String,
    protocol: OriginProtocol,
}

#[allow(non_camel_case_types)]
#[derive(Eq, PartialEq, Columnar)]
pub enum OriginProtocol {
    ORIGIN_PROTOCOL_UNKNOWN,
    HTTP,
    HTTPS,
}

#[allow(non_camel_case_types)]
#[derive(Eq, PartialEq, Columnar)]
pub enum ZonePlan {
    ZONEPLAN_UNKNOWN,
    FREE,
    PRO,
    BIZ,
    ENT,
}

#[derive(Eq, PartialEq, Columnar)]
pub enum Country {
    UNKNOWN,
    A1,
    A2,
    O1,
    AD,
    AE,
    AF,
    AG,
    AI,
    AL,
    AM,
    AO,
    AP,
    AQ,
    AR,
    AS,
    AT,
    AU,
    AW,
    AX,
    AZ,
    BA,
    BB,
    BD,
    BE,
    BF,
    BG,
    BH,
    BI,
    BJ,
    BL,
    BM,
    BN,
    BO,
    BQ,
    BR,
    BS,
    BT,
    BV,
    BW,
    BY,
    BZ,
    CA,
    CC,
    CD,
    CF,
    CG,
    CH,
    CI,
    CK,
    CL,
    CM,
    CN,
    CO,
    CR,
    CU,
    CV,
    CW,
    CX,
    CY,
    CZ,
    DE,
    DJ,
    DK,
    DM,
    DO,
    DZ,
    EC,
    EE,
    EG,
    EH,
    ER,
    ES,
    ET,
    EU,
    FI,
    FJ,
    FK,
    FM,
    FO,
    FR,
    GA,
    GB,
    GD,
    GE,
    GF,
    GG,
    GH,
    GI,
    GL,
    GM,
    GN,
    GP,
    GQ,
    GR,
    GS,
    GT,
    GU,
    GW,
    GY,
    HK,
    HM,
    HN,
    HR,
    HT,
    HU,
    ID,
    IE,
    IL,
    IM,
    IN,
    IO,
    IQ,
    IR,
    IS,
    IT,
    JE,
    JM,
    JO,
    JP,
    KE,
    KG,
    KH,
    KI,
    KM,
    KN,
    KP,
    KR,
    KW,
    KY,
    KZ,
    LA,
    LB,
    LC,
    LI,
    LK,
    LR,
    LS,
    LT,
    LU,
    LV,
    LY,
    MA,
    MC,
    MD,
    ME,
    MF,
    MG,
    MH,
    MK,
    ML,
    MM,
    MN,
    MO,
    MP,
    MQ,
    MR,
    MS,
    MT,
    MU,
    MV,
    MW,
    MX,
    MY,
    MZ,
    NA,
    NC,
    NE,
    NF,
    NG,
    NI,
    NL,
    NO,
    NP,
    NR,
    NU,
    NZ,
    OM,
    PA,
    PE,
    PF,
    PG,
    PH,
    PK,
    PL,
    PM,
    PN,
    PR,
    PS,
    PT,
    PW,
    PY,
    QA,
    RE,
    RO,
    RS,
    RU,
    RW,
    SA,
    SB,
    SC,
    SD,
    SE,
    SG,
    SH,
    SI,
    SJ,
    SK,
    SL,
    SM,
    SN,
    SO,
    SR,
    SS,
    ST,
    SV,
    SX,
    SY,
    SZ,
    TC,
    TD,
    TF,
    TG,
    TH,
    TJ,
    TK,
    TL,
    TM,
    TN,
    TO,
    TR,
    TT,
    TV,
    TW,
    TZ,
    UA,
    UG,
    UM,
    US,
    UY,
    UZ,
    VA,
    VC,
    VE,
    VG,
    VI,
    VN,
    VU,
    WF,
    WS,
    XX,
    YE,
    YT,
    ZA,
    ZM,
    ZW,
}

#[derive(Eq, PartialEq, Columnar)]
pub struct Log {
    timestamp: i64,
    zone_id: u32,
    zone_plan: ZonePlan,
    http: Http,
    origin: Origin,
    country: Country,
    cache_status: CacheStatus,
    server_ip: String,
    server_name: String,
    remote_ip: String,
    bytes_dlv: u64,
    ray_id: String,
}

impl Log {
    pub fn new() -> Log {
        Log {
            timestamp: 2837513946597,
            zone_id: 123456,
            zone_plan: ZonePlan::FREE,
            http: Http {
                protocol: HttpProtocol::HTTP11,
                status: 200,
                host_status: 503,
                up_status: 520,
                method: HttpMethod::GET,
                content_type: "text/html".to_owned(),
                user_agent: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.146 Safari/537.36".to_owned(),
                referer: "https://www.cloudflare.com/".to_owned(),
                request_uri: "/cdn-cgi/trace".to_owned(),
            },
            origin: Origin {
                ip: "1.2.3.4".to_owned(),
                port: 8000,
                hostname: "www.example.com".to_owned(),
                protocol: OriginProtocol::HTTPS,
            },
            country: Country::US,
            cache_status: CacheStatus::Hit,
            server_ip: "192.168.1.1".to_owned(),
            server_name: "metal.cloudflare.com".to_owned(),
            remote_ip: "10.1.2.3".to_owned(),
            bytes_dlv: 123456,
            ray_id: "10c73629cce30078-LAX".to_owned(),
        }
    }
}

benchmark_group!(
    serde,
    bench_new,
    bench_push,
    bench_encode,
    bench_decode,
);
benchmark_main!(serde);
