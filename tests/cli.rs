use std::process::Command;

#[test]
fn test_generate_validate() {
    let output = Command::new(env!("CARGO_BIN_EXE_randstream"))
        .args([
            "generate",
            "--size",
            "1M",
            "--chunk-size",
            "1K",
            "--seed",
            "1234",
            "test.bin",
        ])
        .output()
        .expect("failed to execute generate process");
    assert!(output.status.success());

    let output = Command::new(env!("CARGO_BIN_EXE_randstream"))
        .args([
            "validate",
            "--chunk-size",
            "1K",
            "test.bin",
        ])
        .output()
        .expect("failed to execute validate process");

    println!("status: {}", output.status);
    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
    println!("stderr: {}", String::from_utf8_lossy(&output.stderr));

    assert!(output.status.success());

    std::fs::remove_file("test.bin").expect("failed to remove test file");
}
