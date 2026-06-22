// swiftshare - Web UI JavaScript

const API_BASE = window.location.origin;
let selectedFiles = [];
let ws = null;

// DOM elements
const dropZone = document.getElementById("drop-zone");
const browseBtn = document.getElementById("browse-btn");
const fileInput = document.getElementById("file-input");
const folderInput = document.getElementById("folder-input");
const filePreview = document.getElementById("file-preview");
const fileList = document.getElementById("file-list");
const sendBtn = document.getElementById("send-btn");
const cancelBtn = document.getElementById("cancel-btn");
const peerList = document.getElementById("peer-list");
const manualIp = document.getElementById("manual-ip");
const manualPort = document.getElementById("manual-port");
const connectBtn = document.getElementById("connect-btn");
const transferList = document.getElementById("transfer-list");

// Initialize
document.addEventListener("DOMContentLoaded", () => {
    setupDragAndDrop();
    setupBrowseButtons();
    setupConnectButton();
    setupSendButton();
    setupCancelButton();
    setupWebSocket();
    refreshPeers();
    refreshTransfers();
});

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

    ws.onerror = () => {
        console.error("WebSocket error");
    };

    ws.onclose = () => {
        console.log("WebSocket disconnected, reconnecting...");
        setTimeout(setupWebSocket, 2000);
    };
}

// Drag and drop
function setupDragAndDrop() {
    ["dragenter", "dragover", "dragleave", "drop"].forEach((eventName) => {
        dropZone.addEventListener(eventName, preventDefaults, false);
    });

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    ["dragenter", "dragover"].forEach((eventName) => {
        dropZone.addEventListener(eventName, () => {
            dropZone.classList.add("drag-over");
        });
    });

    ["dragleave", "drop"].forEach((eventName) => {
        dropZone.addEventListener(eventName, () => {
            dropZone.classList.remove("drag-over");
        });
    });

    dropZone.addEventListener("drop", handleDrop);
}

function handleDrop(e) {
    const items = e.dataTransfer.items;
    collectFilesFromItems(items, (files) => {
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
    browseBtn.addEventListener("click", () => {
        // Toggle between file and folder input
        if (fileInput.files.length > 0) {
            folderInput.click();
        } else {
            fileInput.click();
        }
    });

    fileInput.addEventListener("change", (e) => {
        selectedFiles = Array.from(e.target.files);
        showFilePreview(selectedFiles);
    });

    folderInput.addEventListener("change", (e) => {
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

    filePreview.querySelector("h3").textContent =
        `${files.length} archivo(s) - ${formatSize(totalSize)}`;
    filePreview.classList.remove("hidden");
}

// Connect to manual IP
function setupConnectButton() {
    connectBtn.addEventListener("click", async () => {
        const ip = manualIp.value.trim();
        const port = parseInt(manualPort.value) || 45678;

        if (!ip) {
            alert("Ingresa una IP válida");
            return;
        }

        const ipRegex = /^\d{1,3}(\.\d{1,3}){3}$/;
        if (!ipRegex.test(ip)) {
            alert("El formato de IP debe ser: xxx.xxx.xxx.xxx");
            return;
        }

        connectBtn.disabled = true;
        connectBtn.textContent = "Conectando...";

        try {
            const response = await fetch(`${API_BASE}/api/peers/connect`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ ip, port }),
            });

            const data = await response.json();

            if (response.ok && data.status === "discovering") {
                console.log("Discovery sent to", ip);
                setTimeout(refreshPeers, 2000);
            } else {
                alert("Error: " + (data.error || "No se pudo conectar"));
            }
        } catch (e) {
            alert("Error de conexión: " + e.message);
        } finally {
            connectBtn.disabled = false;
            connectBtn.textContent = "Conectar";
        }
    });
}

// Send button
function setupSendButton() {
    sendBtn.addEventListener("click", async () => {
        if (selectedFiles.length === 0) return;

        const peerItems = peerList.querySelectorAll(".peer-item");
        if (peerItems.length === 0) {
            alert("No hay dispositivos disponibles. Asegúrate de que ambos dispositivos estén en la misma red.");
            return;
        }

        const targetPeer = peerItems[0];
        const peerData = JSON.parse(targetPeer.dataset.peerInfo || "{}");

        if (!peerData.tcpHost) {
            alert("No se pudo obtener la dirección IP del dispositivo destino.");
            return;
        }

        sendBtn.disabled = true;
        sendBtn.textContent = `Enviando ${selectedFiles.length} archivo(s)...`;

        try {
            const formData = new FormData();
            for (const file of selectedFiles) {
                formData.append("files", file, file.name);
                addTransferItem(file.name, peerData.alias, "sending");
            }

            const response = await fetch(`${API_BASE}/api/send`, {
                method: "POST",
                body: formData,
            });

            const result = await response.json();

            if (result.status === "saved") {
                selectedFiles.forEach((file, i) => {
                    updateTransferStatus(file.name, "sending", 100, "Completado");
                });
            } else {
                selectedFiles.forEach((file) => {
                    updateTransferStatus(file.name, "failed", 0, "Error al guardar");
                });
            }
        } catch (e) {
            console.error("Send error:", e);
            selectedFiles.forEach((file) => {
                updateTransferStatus(file.name, "failed", 0, "Error de red");
            });
        } finally {
            sendBtn.disabled = false;
            sendBtn.textContent = "Enviar";
        }
    });
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
            if (percentEl) percentEl.textContent = `${percent}%`;
            if (statusEl) {
                statusEl.className = `transfer-status status-${status}`;
                statusEl.textContent = detail || status;
            }
        }
    });
}

// Cancel button
function setupCancelButton() {
    cancelBtn.addEventListener("click", () => {
        selectedFiles = [];
        filePreview.classList.add("hidden");
        fileList.innerHTML = "";
        fileInput.value = "";
        folderInput.value = "";
    });
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

function renderPeers(peers) {
    if (peers.length === 0) {
        peerList.innerHTML = '<div class="empty-state">No se encontraron dispositivos</div>';
        return;
    }

    peerList.innerHTML = "";
    peers.forEach((peer) => {
        const div = document.createElement("div");
        div.className = "peer-item";
        div.dataset.peer = peer.alias;
        div.dataset.peerInfo = JSON.stringify({
            alias: peer.alias,
            tcpPort: peer.tcp_port,
            tcpHost: "127.0.0.1",
        });
        div.innerHTML = `
            <div class="peer-info">
                <div class="peer-status"></div>
                <span class="peer-name">${peer.alias}</span>
            </div>
            <span class="peer-details">Puerto TCP: ${peer.tcp_port}</span>
        `;
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
        transferList.innerHTML =
            '<div class="empty-state">No hay transferencias activas</div>';
        return;
    }

    transferList.innerHTML = "";
    transfers.forEach((transfer) => {
        transfer.forEach((file) => {
            addTransferItem(
                file.name,
                transfer.peer_alias,
                "sending",
                transfer.session_id
            );
        });
    });
}

function addTransferItem(fileName, peer, status, sessionId) {
    // Remove existing item for same session
    const existing = transferList.querySelector(`[data-session="${sessionId}"]`);
    if (existing) {
        existing.remove();
    }

    const div = document.createElement("div");
    div.className = "transfer-item";
    div.dataset.session = sessionId;
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
    const percent = data.total > 0 ? ((data.bytes / data.total) * 100).toFixed(1) : 0;
    const speed = calculateSpeed(data);

    // Find matching transfer item
    const items = transferList.querySelectorAll(".transfer-item");
    items.forEach((item) => {
        const fill = item.querySelector(".progress-fill");
        const percentEl = item.querySelector(".transfer-percent");
        if (fill && percentEl) {
            fill.style.width = `${percent}%`;
            percentEl.textContent = `${percent}% - ${speed}`;
        }
    });

    // Auto-refresh peers when progress updates
    refreshPeers();
}

function calculateSpeed(data) {
    // Simple speed calculation (would need timestamp tracking for real speed)
    const remaining = data.total > data.bytes ? ((data.total - data.bytes) / (data.total || 1)) * 5 : 0;
    return remaining > 0 ? `${remaining.toFixed(1)}s restantes` : "Completado";
}

// Utility: format file size
function formatSize(bytes) {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
}

// Periodic refresh
setInterval(refreshPeers, 3000);
setInterval(refreshTransfers, 2000);
