//! 段ごとの所要 (ミリ秒) を測り、応答の `Server-Timing` に載せる形にする (Refs ippoan/rust-alc-api#725)。
//!
//! 時計は差し込む ([`Clock`]。worker ではランタイムの時計、テストでは固定の歩幅で進む偽物)。載せるのは
//! **固定の語の段の名前と数字だけ** (件数・id・key は載せない)。
//!
//! Workers の時計は I/O をまたがないと進まないので、I/O を含まない段 (zip の展開と parse など) は 0 に見え、
//! その時間は次の I/O を含む段に乗る。

/// 今の時刻 (ミリ秒)。差だけを使う。
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// 段の所要を、終わった順に溜める。
pub struct StageTimer<'a> {
    clock: &'a dyn Clock,
    last: u64,
    stages: Vec<(&'static str, u64)>,
}

impl<'a> StageTimer<'a> {
    pub fn new(clock: &'a dyn Clock) -> Self {
        let last = clock.now_ms();
        let stages = Vec::new();
        Self {
            clock,
            last,
            stages,
        }
    }

    /// 前の区切り (か、作ったとき) から今までを、段 `name` の所要として記録する。
    pub fn lap(&mut self, name: &'static str) {
        let now = self.clock.now_ms();
        self.stages.push((name, now.saturating_sub(self.last)));
        self.last = now;
    }

    /// `Server-Timing` の値 (`history;dur=12, put_zip;dur=34` の形)。段が 1 つも無ければ `None`。
    pub fn server_timing(&self) -> Option<String> {
        let entry = |(name, ms): &(&'static str, u64)| format!("{name};dur={ms}");
        let entries: Vec<String> = self.stages.iter().map(entry).collect();
        Some(entries.join(", ")).filter(|value| !value.is_empty())
    }
}
