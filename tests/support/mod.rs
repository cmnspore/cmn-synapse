//! Process isolation for tests that exercise environment-based key overrides.

/// Return true after the parent has run this exact test in an isolated child.
/// The child receives its environment before the test runtime starts.
pub fn run_with_key_override(key: &str) -> bool {
    let thread = std::thread::current();
    let test_name = thread.name().expect("named test thread");
    if std::env::var("CMN_TEST_KEY_OVERRIDE_CHILD").as_deref() == Ok(test_name) {
        assert_eq!(std::env::var("CMN_PUBLIC_KEY_OVERRIDE").as_deref(), Ok(key));
        return false;
    }

    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture"])
        .env("CMN_TEST_KEY_OVERRIDE_CHILD", test_name)
        .env("CMN_PUBLIC_KEY_OVERRIDE", key)
        .output()
        .expect("run isolated key-override test");
    assert!(
        output.status.success(),
        "{test_name} failed in child process:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
        "child did not execute exactly one test: {test_name}",
    );
    true
}
