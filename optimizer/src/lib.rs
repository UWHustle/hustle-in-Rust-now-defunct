use std::process::Command;
use std::path::PathBuf;

pub fn optimize(sql: &str) -> Result<String, String> {
    let out_dir = env!("OUT_DIR");
    let mut optimizer_exe = PathBuf::from(out_dir);
    optimizer_exe.push("optimizer");

    let output = Command::new(optimizer_exe)
        .arg(sql)
        .output()
        .map_err(|e| e.to_string())?;

    if output.status.success() {
        let output = String::from_utf8(output.stdout).unwrap();
        if output.contains("ERROR") {
            Err(output)
        } else {
            Ok(output)
        }
    } else {
        Err(String::from_utf8(output.stderr).unwrap())
    }
}
