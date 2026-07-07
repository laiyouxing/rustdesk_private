with open('libs/hbb_common/src/lib.rs', 'r', encoding='utf-8') as f:
    c = f.read()

old = '''pub fn gen_version() {
    println!("cargo:rerun-if-changed=Cargo.toml");
    use std::io::prelude::*;
    let mut file = File::create("./src/version.rs").unwrap();
    for line in read_lines("Cargo.toml").unwrap().flatten() {
        let ab: Vec<&str> = line.split('=').map(|x| x.trim()).collect();
        if ab.len() == 2 && ab[0] == "version" {
            file.write_all(format!("pub const VERSION: &str = {};\\n", ab[1]).as_bytes())
                .ok();
            break;
        }
    }'''

new = '''pub fn gen_version() {
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-env-changed=BUILD_VERSION");
    use std::io::prelude::*;
    let mut file = File::create("./src/version.rs").unwrap();
    // Allow BUILD_VERSION env var to override Cargo.toml version (used by CI tag builds)
    let version = if let Ok(bv) = std::env::var("BUILD_VERSION") {
        format!("\\"{}\\"", bv)
    } else {
        for line in read_lines("Cargo.toml").unwrap().flatten() {
            let ab: Vec<&str> = line.split('=').map(|x| x.trim()).collect();
            if ab.len() == 2 && ab[0] == "version" {
                break ab[1].to_owned();
            }
        }
        .unwrap_or_else(|| "\\"unknown\\"".to_owned())
    };
    file.write_all(format!("pub const VERSION: &str = {};\\n", version).as_bytes())
        .ok();'''

c = c.replace(old, new)

with open('libs/hbb_common/src/lib.rs', 'w', encoding='utf-8') as f:
    f.write(c)

print('Fixed gen_version()')
