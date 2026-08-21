fn main() {
    tauri_build::try_build(
        tauri_build::Attributes::new().app_manifest(
            tauri_build::AppManifest::new().commands(&["pick_folder_files", "read_file_bytes"]),
        ),
    )
    .expect("failed to run tauri-build");
}
