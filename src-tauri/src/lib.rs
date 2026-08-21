// Desktop shell around the swiftshare library: a native window instead of a
// browser tab, a tray icon so it can keep receiving in the background, and
// OS notifications driven straight off the same event stream the web UI's
// WebSocket already uses.

use std::path::Path;

use clap::Parser;
use serde::Serialize;
use tauri::menu::{Menu, MenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{AppHandle, Manager, WindowEvent};
use tauri_plugin_dialog::DialogExt;
use tauri_plugin_notification::NotificationExt;

use swiftshare::cli::Cli;
use swiftshare::state::{Direction, Event, TransferStatus};

/// One file found under a picked folder. `std::fs::read_dir` doesn't filter
/// dotfiles the way the browser's directory-upload/drag-drop APIs do, so
/// picking folders through this native dialog is how hidden files (`.env`,
/// `.git/...`) make it into a transfer.
#[derive(Serialize)]
struct FolderFile {
    rel_path: String,
    abs_path: String,
    size: u64,
}

#[tauri::command]
async fn pick_folder_files(app: AppHandle) -> Option<Vec<FolderFile>> {
    // `blocking_pick_folder` must not be called from the main thread (it
    // blocks waiting on the very event loop it's holding up), and Tauri runs
    // sync commands on the main thread — hence the callback + channel dance
    // to await the non-blocking picker instead.
    let (tx, rx) = tokio::sync::oneshot::channel();
    app.dialog().file().pick_folder(move |folder| {
        let _ = tx.send(folder);
    });
    let root = rx.await.ok()??.into_path().ok()?;
    let mut files = Vec::new();
    collect_files(&root, &root, &mut files);
    Some(files)
}

fn collect_files(root: &Path, dir: &Path, out: &mut Vec<FolderFile>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        // file_type() doesn't follow symlinks (unlike path.is_dir()/metadata());
        // skipping them avoids recursing into a symlink loop and hanging.
        let Ok(file_type) = entry.file_type() else { continue };
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            collect_files(root, &path, out);
        } else if let Ok(meta) = entry.metadata() {
            out.push(FolderFile {
                rel_path: path.strip_prefix(root).unwrap_or(&path).to_string_lossy().replace('\\', "/"),
                abs_path: path.to_string_lossy().into_owned(),
                size: meta.len(),
            });
        }
    }
}

#[tauri::command]
async fn read_file_bytes(path: String) -> Result<tauri::ipc::Response, String> {
    // Sync commands run on the main thread; a plain std::fs::read here would
    // freeze the whole app (including any incoming-transfer modal) for every
    // file read while a folder is being queued to send.
    tokio::fs::read(&path).await.map(tauri::ipc::Response::new).map_err(|e| e.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    tauri::Builder::default()
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            // A second launch (double-clicking the app again, a second CLI
            // invocation) should surface the existing window, not start a
            // second copy competing for the same TCP/UDP ports.
            focus_main_window(app);
        }))
        .invoke_handler(tauri::generate_handler![pick_folder_files, read_file_bytes])
        .setup(|app| {
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move { start_backend(handle).await });
            build_tray(app.handle())?;
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                // The red close button hides the window; swiftshare keeps
                // running (and receiving) in the tray, same as it would as a
                // background CLI process. Quit is the tray menu's job.
                let _ = window.hide();
                api.prevent_close();
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running swiftshare");
}

fn focus_main_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.set_focus();
    }
}

/// Starts the actual swiftshare backend (discovery + TCP transfer + web UI)
/// and points the native window at it once the UI's real port is known —
/// `--http-port 0` lets the OS pick one so it never collides with anything
/// already using 8080 on this machine.
async fn start_backend(handle: AppHandle) {
    let cli = Cli::parse_from(["swiftshare", "--http-port", "0"]);

    let running = match swiftshare::start(&cli).await {
        Ok(running) => running,
        Err(e) => {
            tracing::error!("swiftshare failed to start: {:#}", e);
            return;
        }
    };

    if let Some(window) = handle.get_webview_window("main") {
        let url = format!("http://{}", running.http_addr);
        match url.parse() {
            Ok(parsed) => match window.navigate(parsed) {
                Ok(()) => {
                    tracing::info!("Window navigated to {}", url);
                    // A window launched from a background process doesn't
                    // always steal focus on its own; make sure the app the
                    // user just opened is actually the one on screen.
                    let _ = window.show();
                    let _ = window.set_focus();
                }
                Err(e) => tracing::error!("Failed to navigate window to {}: {}", url, e),
            },
            Err(e) => tracing::error!("Bad UI URL {}: {}", url, e),
        }
    }

    let mut events = running.state.events.subscribe();
    while let Ok(event) = events.recv().await {
        notify_for_event(&handle, event);
    }
}

fn notify_for_event(handle: &AppHandle, event: Event) {
    let Some((title, body)) = describe_event(event) else {
        return;
    };
    if let Err(e) = handle.notification().builder().title(&title).body(&body).show() {
        tracing::error!("Failed to show notification ({}: {}): {}", title, body, e);
    } else {
        tracing::info!("Notification shown: {} — {}", title, body);
    }
}

fn describe_event(event: Event) -> Option<(String, String)> {
    match event {
        Event::Incoming { peer, files, total_size, .. } => Some((
            "Transferencia entrante".to_string(),
            format!("{} quiere enviarte {} archivo(s) ({})", peer, files.len(), format_size(total_size)),
        )),
        Event::SessionDone { direction, peer, status, file_count, .. } => match (direction, status) {
            (Direction::Recv, TransferStatus::Completed) => Some((
                "Transferencia completada".to_string(),
                format!("Recibiste {} archivo(s) de {}", file_count, peer),
            )),
            (Direction::Recv, TransferStatus::Failed { error }) => {
                Some(("Transferencia fallida".to_string(), format!("{}: {}", peer, error)))
            }
            (Direction::Send, TransferStatus::Rejected) => Some((
                "Transferencia rechazada".to_string(),
                format!("{} rechazó la transferencia", peer),
            )),
            (_, TransferStatus::Cancelled) => {
                Some(("Transferencia cancelada".to_string(), format!("Con {}", peer)))
            }
            _ => None,
        },
        _ => None,
    }
}

fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    format!("{:.1} {}", size, UNITS[unit])
}

fn build_tray(app: &AppHandle) -> tauri::Result<()> {
    let open_item = MenuItem::with_id(app, "open", "Abrir swiftshare", true, None::<&str>)?;
    let quit_item = MenuItem::with_id(app, "quit", "Salir", true, None::<&str>)?;
    let menu = Menu::with_items(app, &[&open_item, &quit_item])?;

    // Black-on-transparent so macOS can tint it for light/dark menu bars.
    let icon = tauri::image::Image::from_bytes(include_bytes!("../icons/tray.png"))?;

    TrayIconBuilder::new()
        .icon(icon)
        .icon_as_template(true)
        .menu(&menu)
        .show_menu_on_left_click(true)
        .on_menu_event(|app, event| match event.id.as_ref() {
            "open" => focus_main_window(app),
            "quit" => app.exit(0),
            _ => {}
        })
        .build(app)?;

    Ok(())
}
