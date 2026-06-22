// swiftshare - Web UI

const API_BASE = window.location.origin;
let selectedFiles = [];
let selectedPeer = null;
let ws = null;

// DOM elements
const dropZone = document.getElementById("drop-zone");
const browseBtn = document.getElementById("browse-btn");
const fileInput = document.getElementById("file-input");
const filePreview = document.getElementById("file-preview");
const fileList = document.getElementById("file-list");
const fileCount = document.getElementById("file-count");
const sendBtn = document.getElementById("send-btn");
const cancelBtn = document.getElementById("cancel-btn");
const peerList = document.getElementById("peer-list");
const manualIp = document.getElementById("manual-ip");
const connectBtn = document.getElementById("connect-btn");
const transferList = document.getElementById("transfer-list");
const searchBadge = document.getElementById("search-badge");
const aliasEl = document.getElementById("alias");

// Initialize
document.addEventListener("DOMContentLoaded", async () => {
    setupDragAndDrop();
    setupBrowseButtons();
    setupConnectButton();
    setupSendButton();
    setupCancelButton();
    setupWebSocket();
    await refreshPeers();
    await refreshTransfers();
    showToast("Los archivos se guardan en ~/Downloads/.swiftshare-temp/", "info");
});

// Toast notifications
function showToast(message, type = "info") {
    const container = document.getElementById("toast-container");
    const toast = document.createElement("div");
    toast.className = `toast ${type}`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => toast.remove(), 4000);
}

// WebSocket for real-time progress
function setupWebSocket() {
    const wsUrl = `ws://${window.location.host}/api/ws`;
    ws = new WebSocket(wsUrl);

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            updateProgress(data);
        } catch (e) {
            console.error("Failed to parse WebSocket message:", e);
        }
    };

    ws.onerror = () => console.error("WebSocket error");
    ws.onclose = () => {
        setTimeout(setupWebSocket, 2000);
    };
}

// Drag and drop
function setupDragAndDrop() {
    ["dragenter", "dragover", "dragleave", "drop"].forEach((name) => {
        dropZone.addEventListener(name, (e) => {
            e.preventDefault();
            e.stopPropagation();
        });
    });

    ["dragenter", "dragover"].forEach((name) => {
        dropZone.addEventListener(name, () => {
            dropZone.classList.add("drag-over");
        });
    });

    ["dragleave", "drop"].forEach((name) => {
        dropZone.addEventListener(name, () => {
            dropZone.classList.remove("drag-over");
        });
    });

    dropZone.addEventListener("drop", handleDrop);
}

async function handleDrop(e) {
    const items = e.dataTransfer.items;
    await collectFilesFromItems(items, (files) => {
        selectedFiles = files;
        showFilePreview(files);
    });
}

async function collectFilesFromItems(items, callback) {
    const files = [];
    for (const item of items) {
        if (item.kind === "file") {
            const entry = item.webkitGetAsEntry?.();
            if (entry) {
                await collectEntry(entry, files);
            }
        }
    }
    callback(files);
}

async function collectEntry(entry, files) {
    if (entry.isFile) {
        const file = await new Promise((resolve) => entry.file(resolve));
        files.push(file);
    } else if (entry.isDirectory) {
        const reader = entry.createReader();
        const entries = await new Promise((resolve) => {
            reader.readEntries((entries) => resolve(entries));
        });
        for (const e of entries) {
            await collectEntry(e, files);
        }
    }
}

// Browse buttons
function setupBrowseButtons() {
    browseBtn.addEventListener("click", () => fileInput.click());
    fileInput.addEventListener("change", (e) => {
        selectedFiles = Array.from(e.target.files);
        showFilePreview(selectedFiles);
    });
}

// Show selected files
function showFilePreview(files) {
    fileList.innerHTML = "";
    let totalSize = 0;

    files.forEach((file) => {
        totalSize += file.size;
        const li = document.createElement("li");
        li.innerHTML = `
            <span class="file-name">${file.name}</span>
            <span class="file-size">${formatSize(file.size)}</span>
        `;
        fileList.appendChild(li);
    });

    fileCount.textContent = `${files.length} archivo(s) · ${formatSize(totalSize)}`;
    filePreview.classList.remove("hidden");
    updateSendButton();
}

// Connect to manual IP
function setupConnectButton() {
    connectBtn.addEventListener("click", async () => {
        const ip = manualIp.value.trim();

        if (!ip) {
            showToast("Ingresa una IP válida", "error");
            return;
        }

        const ipRegex = /^\d{1,3}(\.\d{1,3}){3}$/;
        if (!ipRegex.test(ip)) {
            showToast("Formato IP inválido (ej: 192.168.1.100)", "error");
            return;
        }

        connectBtn.disabled = true;
        connectBtn.textContent = "Buscando...";
        manualIp.disabled = true;

        try {
            const response = await fetch(`${API_BASE}/api/peers/connect`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ ip, tcp_port: 45679 }),
            });

            const data = await response.json();

            if (response.ok) {
                showToast(`Probe enviado a ${ip}`, "info");

                // Poll for peers
                for (let i = 0; i < 4; i++) {
                    await sleep(1500);
                    await refreshPeers();
                    const peers = await getPeers();
                    if (peers.length > 0) {
                        showToast(`¡Dispositivo encontrado!`, "success");
                        connectBtn.textContent = "¡Encontrado!";
                        connectBtn.style.background = "var(--success)";
                        break;
                    }
                }
            } else {
                showToast(data.error || "Error al conectar", "error");
            }
        } catch (e) {
            showToast("Error de red: " + e.message, "error");
        } finally {
            connectBtn.disabled = false;
            connectBtn.textContent = "Conectar";
            connectBtn.style.background = "";
            manualIp.disabled = false;
        }
    });

    // Enter key
    manualIp.addEventListener("keydown", (e) => {
        if (e.key === "Enter") connectBtn.click();
    });
}

// Send button
function setupSendButton() {
    sendBtn.addEventListener("click", async () => {
        if (selectedFiles.length === 0) return;
        if (!selectedPeer) {
            showToast("Selecciona un dispositivo para enviar", "error");
            return;
        }

        sendBtn.disabled = true;
        sendBtn.textContent = `Enviando ${selectedFiles.length} archivo(s)...`;

        let successCount = 0;
        let failCount = 0;

        try {
            for (const file of selectedFiles) {
                addTransferItem(file.name, selectedPeer.alias, "sending");

                const formData = new FormData();
                formData.append("file", file, file.name);

                const url = `${API_BASE}/api/send?target_ip=${encodeURIComponent(selectedPeer.ip)}&target_tcp_port=${selectedPeer.tcp_port}`;
                const response = await fetch(url, {
                    method: "POST",
                    body: formData,
                });

                    if (!response.ok) {
                        const text = await response.text();
                        console.error("Upload failed:", response.status, text);
                        updateTransferStatus(file.name, "failed", 0, `Error ${response.status}`);
                        failCount++;
                        continue;
                    }

                    const result = await response.json();

                    if (result.status === "saved") {
                        updateTransferStatus(file.name, "sending", 100, "Completado ✓");
                        successCount++;
                    } else {
                        updateTransferStatus(file.name, "failed", 0, result.error || "Error");
                        failCount++;
                    }
                } catch (e) {
                    updateTransferStatus(file.name, "failed", 0, "Error de red");
                    failCount++;
                }

                // Brief delay between files
                await sleep(200);
            }

            if (successCount > 0) {
                showToast(`${successCount} archivo(s) enviados correctamente`, "success");
            }
            if (failCount > 0) {
                showToast(`${failCount} archivo(s) fallaron`, "error");
            }
        } catch (e) {
            showToast("Error: " + e.message, "error");
        } finally {
            sendBtn.disabled = false;
            sendBtn.textContent = "Enviar";
        }
    });
}

function setupCancelButton() {
    cancelBtn.addEventListener("click", () => {
        selectedFiles = [];
        filePreview.classList.add("hidden");
        fileList.innerHTML = "";
        fileInput.value = "";
        updateSendButton();
    });
}

function updateSendButton() {
    sendBtn.disabled = !selectedPeer || selectedFiles.length === 0;
}

// Refresh peers list
async function refreshPeers() {
    try {
        const response = await fetch(`${API_BASE}/api/peers`);
        if (!response.ok) return;
        const peers = await response.json();
        renderPeers(peers);
    } catch (e) {
        console.error("Failed to refresh peers:", e);
    }
}

async function getPeers() {
    try {
        const res = await fetch(`${API_BASE}/api/peers`);
        if (!res.ok) return [];
        return await res.json();
    } catch {
        return [];
    }
}

function renderPeers(peers) {
    if (peers.length === 0) {
        peerList.innerHTML = `
            <div class="empty-state">
                No se encontraron dispositivos
                <div class="hint">Asegúrate de que ambos dispositivos estén en la misma red</div>
            </div>
        `;
        searchBadge.textContent = "Sin dispositivos";
        searchBadge.classList.remove("searching");
        searchBadge.classList.add("online");
        searchBadge.style.background = "rgba(239, 68, 68, 0.2)";
        searchBadge.style.color = "var(--error)";
        return;
    }

    searchBadge.textContent = `${peers.length} dispositivo(s)`;
    searchBadge.classList.remove("searching");
    searchBadge.style.background = "rgba(34, 197, 94, 0.2)";
    searchBadge.style.color = "var(--success)";

    peerList.innerHTML = "";
    peers.forEach((peer) => {
        const div = document.createElement("div");
        div.className = "peer-item" + (selectedPeer?.fingerprint === peer.fingerprint ? " selected" : "");
        div.innerHTML = `
            <div class="peer-info">
                <div class="peer-status"></div>
                <span class="peer-name">${peer.alias}</span>
            </div>
            <span class="peer-details">TCP: ${peer.tcp_port}</span>
        `;
        div.addEventListener("click", () => {
            selectedPeer = peer;
            renderPeers(peers);
            updateSendButton();
            showToast(`Seleccionado: ${peer.alias}`, "info");
        });
        peerList.appendChild(div);
    });
}

// Refresh transfers list
async function refreshTransfers() {
    try {
        const response = await fetch(`${API_BASE}/api/transfers`);
        if (!response.ok) return;
        const transfers = await response.json();
        renderTransfers(transfers);
    } catch (e) {
        console.error("Failed to refresh transfers:", e);
    }
}

function renderTransfers(transfers) {
    if (transfers.length === 0) {
        transferList.innerHTML = '<div class="empty-state">Sin transferencias activas</div>';
        return;
    }

    transferList.innerHTML = "";
    transfers.forEach((transfer) => {
        transfer.forEach((file) => {
            addTransferItem(file.name, transfer.peer_alias, "sending");
        });
    });
}

function addTransferItem(fileName, peer, status) {
    const div = document.createElement("div");
    div.className = "transfer-item";
    div.innerHTML = `
        <div class="transfer-header">
            <span class="transfer-file">${fileName}</span>
            <span class="transfer-peer">→ ${peer}</span>
        </div>
        <div class="progress-bar">
            <div class="progress-fill" style="width: 0%"></div>
        </div>
        <div class="transfer-meta">
            <span class="transfer-status status-${status}">${status}</span>
            <span class="transfer-percent">0%</span>
        </div>
    `;
    transferList.appendChild(div);
}

function updateProgress(data) {
    const percent = data.total > 0 ? ((data.bytes / data.total) * 100).toFixed(0) : 0;
    const speed = formatSpeed(data.bytes);

    const items = transferList.querySelectorAll(".transfer-item");
    items.forEach((item) => {
        const fill = item.querySelector(".progress-fill");
        const percentEl = item.querySelector(".transfer-percent");
        if (fill) fill.style.width = `${percent}%`;
        if (percentEl) percentEl.textContent = `${percent}% · ${speed}`;
    });

    if (parseInt(percent) === 100) {
        showToast("Transferencia completada", "success");
    }
}

function formatSpeed(bytes) {
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1048576) return (bytes / 1024).toFixed(0) + " KB";
    return (bytes / 1048576).toFixed(1) + " MB";
}

function updateTransferStatus(fileName, status, percent, detail) {
    const items = transferList.querySelectorAll(".transfer-item");
    items.forEach((item) => {
        const fileEl = item.querySelector(".transfer-file");
        if (fileEl && fileEl.textContent === fileName) {
            const fill = item.querySelector(".progress-fill");
            const percentEl = item.querySelector(".transfer-percent");
            const statusEl = item.querySelector(".transfer-status");
            if (fill) fill.style.width = `${percent}%`;
            if (percentEl) percentEl.textContent = detail || `${percent}%`;
            if (statusEl) {
                statusEl.className = `transfer-status status-${status}`;
                statusEl.textContent = detail || status;
            }
        }
    });
}

function formatSize(bytes) {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// Periodic refresh
setInterval(refreshPeers, 3000);
setInterval(refreshTransfers, 2000);
