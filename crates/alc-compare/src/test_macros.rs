#[cfg(coverage)]
macro_rules! test_case {
    ($desc:expr, $body:expr) => {
        $body
    };
}

#[cfg(not(coverage))]
macro_rules! test_case {
    ($desc:expr, $body:expr) => {{
        print!("  ✅ {} ... ", $desc);
        std::io::Write::flush(&mut std::io::stdout()).ok();
        let val = $body;
        println!("OK");
        val
    }};
}

#[cfg(coverage)]
macro_rules! test_group {
    ($name:expr) => {};
}

#[cfg(not(coverage))]
macro_rules! test_group {
    ($name:expr) => {
        println!("\n📋 {}", $name);
    };
}

#[cfg(coverage)]
macro_rules! test_section {
    ($name:expr) => {};
}

#[cfg(not(coverage))]
macro_rules! test_section {
    ($name:expr) => {
        println!("\n  ── {} ──", $name);
    };
}

#[cfg(coverage)]
macro_rules! test_info {
    ($($arg:tt)*) => {};
}

#[cfg(not(coverage))]
macro_rules! test_info {
    ($($arg:tt)*) => {
        println!("    💡 {}", format!($($arg)*));
    };
}
