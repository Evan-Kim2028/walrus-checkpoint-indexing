use std::process::Command;

fn main() {
    // Check if walrus CLI is available in PATH
    let walrus_available = Command::new("which")
        .arg("walrus")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if !walrus_available {
        // Also try Windows 'where' command
        let walrus_available_win = Command::new("where")
            .arg("walrus")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        if !walrus_available_win {
            println!("cargo:warning=");
            println!("cargo:warning=Walrus CLI not found in PATH!");
            println!("cargo:warning=");
            println!("cargo:warning=The Walrus CLI is required to download checkpoint blobs.");
            println!("cargo:warning=");
            println!("cargo:warning=Run ./scripts/setup.sh to install automatically,");
            println!(
                "cargo:warning=or install manually from: https://docs.wal.app/usage/setup.html"
            );
            println!("cargo:warning=");
            println!("cargo:warning=After installing, verify with: walrus --version");
            println!("cargo:warning=");
        }
    }
}
