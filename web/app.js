// swiftshare - Web UI

const $ = (id) => document.getElementById(id);

const dropZone = $("drop-zone");
const fileInput = $("file-input");
const folderInput = $("folder-input");
const filePreview = $("file-preview");
const fileList = $("file-list");
const fileCount = $("file-count");
const sendBtn = $("send-btn");
const peerList = $("peer-list");
const manualIp = $("manual-ip");
const connectBtn = $("connect-btn");
const transferList = $("transfer-list");
const receivedList = $("received-list");
const searchBadge = $("search-badge");
const historyList = $("history-list");

let selectedFiles = [];
let selectedPeer = null;
let sending = false;

/** Live rows keyed by `${session_id}:${file_id}` so progress lands on the right bar. */
const rows = new Map();
/** Rate samples per row key, for real speed instead of "bytes so far". */
const rates = new Map();
/** Incoming approval requests, oldest first. Only the head is shown. */
const incomingQueue = [];
/** The upload this tab is currently pushing, so Cancel can also abort the
 *  in-flight fetch instead of waiting for the server to notice. */
const currentUpload = { sessionId: null, controller: null };

// ---------------------------------------------------------------- helpers

function formatSize(bytes) {
    if (!bytes) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    return `${(bytes / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatDuration(seconds) {
    if (!isFinite(seconds) || seconds < 0) return "";
    if (seconds < 60) return `${Math.ceil(seconds)}s`;
    const m = Math.floor(seconds / 60);
    return `${m}m ${Math.ceil(seconds % 60)}s`;
}

/** textContent everywhere: peer aliases and file names come off the network. */
function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
}

function showToast(message, type = "info") {
    const toast = el("div", `toast ${type}`, message);
    $("toast-container").appendChild(toast);
    setTimeout(() => toast.remove(), 4500);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------- notifications

function notify(title, body) {
    if (!("Notification" in window) || Notification.permission !== "granted") return;
    try {
        new Notification(title, { body, icon: "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg'/%3E" });
    } catch {
        // Some browsers only allow notifications from a service worker.
    }
}

/** Short chime, so an incoming request is noticeable in a background tab. */
function chime() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain).connect(ctx.destination);
        osc.frequency.setValueAtTime(880, ctx.currentTime);
        osc.frequency.setValueAtTime(1180, ctx.currentTime + 0.12);
        gain.gain.setValueAtTime(0.12, ctx.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.35);
        osc.start();
        osc.stop(ctx.currentTime + 0.35);
        setTimeout(() => ctx.close(), 600);
    } catch {
        // Autoplay policy blocked it; the modal and toast still show.
    }
}

function setupNotifications() {
    const btn = $("notify-btn");
    const label = btn.querySelector(".btn-toggle-label");
    const paint = () => {
        const granted = "Notification" in window && Notification.permission === "granted";
        label.textContent = granted ? "Notificaciones activas" : "Activar notificaciones";
        btn.classList.toggle("active", granted);
    };
    paint();

    btn.addEventListener("click", async () => {
        if (!("Notification" in window)) return showToast("Este navegador no soporta notificaciones", "error");
        if (Notification.permission === "denied") {
            return showToast("Permiso bloqueado — actívalo en los ajustes del navegador", "error");
        }
        await Notification.requestPermission();
        paint();
        if (Notification.permission === "granted") notify("swiftshare", "Notificaciones activadas");
    });
}

// ---------------------------------------------------------------- websocket

function setupWebSocket(retry = 0) {
    const proto = window.location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${window.location.host}/api/ws`);

    ws.onopen = () => { retry = 0; };
    ws.onmessage = (event) => {
        try {
            handleEvent(JSON.parse(event.data));
        } catch (e) {
            console.error("Bad event:", e);
        }
    };
    ws.onclose = () => {
        // Back off instead of hammering a server that went away.
        setTimeout(() => setupWebSocket(retry + 1), Math.min(1000 * 2 ** retry, 15000));
    };
    ws.onerror = () => ws.close();
}

function handleEvent(event) {
    switch (event.type) {
        case "incoming":
            queueIncoming(event);
            break;
        case "decided":
            dismissIncoming(event.session_id);
            break;
        case "progress":
            updateRow(event);
            break;
        case "fileDone":
            finishRow(event);
            break;
        case "sessionDone":
            handleSessionDone(event);
            break;
    }
}

// ---------------------------------------------------------------- approval modal

function queueIncoming(event) {
    if (incomingQueue.some((e) => e.session_id === event.session_id)) return;
    incomingQueue.push(event);

    const summary = `${event.files.length} archivo(s) · ${formatSize(event.total_size)}`;
    notify("Transferencia entrante", `${event.peer} quiere enviarte ${summary}`);
    chime();
    showToast(`${event.peer} quiere enviarte ${summary}`, "info");
    renderIncoming();
}

function dismissIncoming(sessionId) {
    const i = incomingQueue.findIndex((e) => e.session_id === sessionId);
    if (i !== -1) incomingQueue.splice(i, 1);
    renderIncoming();
}

let countdownTimer = null;

function renderIncoming() {
    const modal = $("incoming-modal");
    if (countdownTimer) clearInterval(countdownTimer);

    const current = incomingQueue[0];
    if (!current) {
        modal.classList.add("hidden");
        return;
    }

    $("incoming-peer").textContent = current.peer;
    $("incoming-summary").textContent =
        `${current.files.length} archivo(s) · ${formatSize(current.total_size)}`;

    const list = $("incoming-files");
    list.replaceChildren();
    current.files.slice(0, 12).forEach((f) => {
        const li = el("li");
        li.appendChild(el("span", "modal-file-name", f.name));
        li.appendChild(el("span", "modal-file-size", formatSize(f.size)));
        list.appendChild(li);
    });
    if (current.files.length > 12) {
        list.appendChild(el("li", "modal-more", `y ${current.files.length - 12} más...`));
    }

    const queue = $("incoming-queue");
    queue.classList.toggle("hidden", incomingQueue.length < 2);
    queue.textContent = `+${incomingQueue.length - 1} solicitud(es) en espera`;

    // Mirrors DECISION_TIMEOUT on the server; purely informational.
    let left = 120;
    $("incoming-countdown").textContent = left;
    countdownTimer = setInterval(() => {
        left -= 1;
        $("incoming-countdown").textContent = Math.max(left, 0);
        if (left <= 0) {
            clearInterval(countdownTimer);
            dismissIncoming(current.session_id);
        }
    }, 1000);

    modal.classList.remove("hidden");
}

async function decide(accept) {
    const current = incomingQueue[0];
    if (!current) return;
    dismissIncoming(current.session_id);

    try {
        const res = await fetch("/api/decision", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ session_id: current.session_id, accept }),
        });
        const data = await res.json();
        if (data.status === "error") showToast(data.error, "error");
        else showToast(accept ? "Transferencia aceptada" : "Transferencia rechazada", accept ? "success" : "info");
    } catch (e) {
        showToast("No se pudo responder: " + e.message, "error");
    }
    refreshTransfers();
}

// ---------------------------------------------------------------- transfers

const rowKey = (sessionId, fileId) => `${sessionId}:${fileId}`;

async function cancelTransfer(sessionId) {
    // Stop the local upload immediately rather than waiting for the server
    // to notice on its next chunk.
    if (currentUpload.sessionId === sessionId && currentUpload.controller) {
        currentUpload.controller.abort();
    }
    try {
        const res = await fetch("/api/transfers/cancel", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ session_id: sessionId }),
        });
        const data = await res.json();
        if (data.status === "error") showToast(data.error, "error");
    } catch (e) {
        showToast("Error de red: " + e.message, "error");
    }
}

function buildRow(key, name, peer, direction, sessionId) {
    const item = el("div", "transfer-item");

    const header = el("div", "transfer-header");
    header.appendChild(el("span", "transfer-file", name));
    header.appendChild(el("span", "transfer-peer", direction === "send" ? `→ ${peer}` : `← ${peer}`));

    const bar = el("div", "progress-bar");
    const fill = el("div", "progress-fill");
    bar.appendChild(fill);

    const meta = el("div", "transfer-meta");
    const status = el("span", "transfer-status", "Pendiente");
    const detail = el("span", "transfer-percent", "0%");
    const cancelBtn = el("button", "btn-cancel", "Cancelar");
    cancelBtn.addEventListener("click", () => cancelTransfer(sessionId));
    meta.append(status, detail, cancelBtn);

    item.append(header, bar, meta);

    const row = { item, fill, status, detail, cancelBtn, done: false };
    rows.set(key, row);
    return row;
}

/** Hides the Cancel button once a row can no longer be cancelled. */
function markRowDone(row) {
    row.done = true;
    row.cancelBtn.classList.add("hidden");
}

function ensureRow(sessionId, fileId, name, peer, direction) {
    const key = rowKey(sessionId, fileId);
    let row = rows.get(key);
    if (!row) {
        row = buildRow(key, name, peer, direction, sessionId);
        const empty = transferList.querySelector(".empty-state");
        if (empty) empty.remove();
        transferList.prepend(row.item);
    }
    return row;
}

/** Exponentially smoothed bytes/sec from consecutive progress samples. */
function trackRate(key, bytes) {
    const now = performance.now();
    const prev = rates.get(key);
    if (!prev) {
        rates.set(key, { bytes, at: now, bps: 0 });
        return 0;
    }
    const dt = (now - prev.at) / 1000;
    if (dt < 0.05) return prev.bps;

    const instant = Math.max(bytes - prev.bytes, 0) / dt;
    const bps = prev.bps ? prev.bps * 0.7 + instant * 0.3 : instant;
    rates.set(key, { bytes, at: now, bps });
    return bps;
}

function updateRow(event) {
    const key = rowKey(event.session_id, event.file_id);
    const row = ensureRow(event.session_id, event.file_id, event.name, "", event.direction);
    if (row.done) return;

    const pct = event.total > 0 ? Math.min((event.bytes / event.total) * 100, 100) : 0;
    const bps = trackRate(key, event.bytes);
    const eta = bps > 0 ? (event.total - event.bytes) / bps : Infinity;

    row.fill.style.width = `${pct}%`;
    row.status.className = "transfer-status status-active";
    row.status.textContent = event.direction === "send" ? "Enviando" : "Recibiendo";
    row.detail.textContent =
        `${pct.toFixed(0)}% · ${formatSize(bps)}/s` + (eta > 0 && isFinite(eta) ? ` · ${formatDuration(eta)}` : "");
}

function finishRow(event) {
    const key = rowKey(event.session_id, event.file_id);
    const row = rows.get(key);
    rates.delete(key);
    if (!row) return;

    markRowDone(row);
    if (event.error?.toLowerCase().includes("cancel")) {
        row.status.className = "transfer-status status-failed";
        row.status.textContent = "Cancelado";
        row.detail.textContent = event.error;
        row.fill.classList.add("failed");
    } else if (event.error) {
        row.status.className = "transfer-status status-failed";
        row.status.textContent = "Error";
        row.detail.textContent = event.error;
        row.fill.classList.add("failed");
    } else {
        row.status.className = "transfer-status status-done";
        row.status.textContent = "Completado";
        row.detail.textContent = "100%";
        row.fill.style.width = "100%";
    }
}

function handleSessionDone(event) {
    const state = event.status.state;
    if (event.direction === "recv") {
        if (state === "completed") {
            notify("Transferencia completada", `Recibiste ${event.file_count} archivo(s) de ${event.peer}`);
            showToast(`Recibiste ${event.file_count} archivo(s) de ${event.peer}`, "success");
            refreshReceived();
        } else if (state === "failed") {
            showToast(`Transferencia de ${event.peer} falló: ${event.status.error}`, "error");
        } else if (state === "cancelled") {
            showToast(`Transferencia con ${event.peer} cancelada`, "info");
        }
    } else if (state === "rejected") {
        showToast(`${event.peer} rechazó la transferencia`, "error");
    } else if (state === "cancelled") {
        showToast(`Envío a ${event.peer} cancelado`, "info");
    }
    refreshTransfers();
    // The session just landed in sqlite; if the history tab is open, show it.
    if (!$("view-history").classList.contains("hidden")) refreshHistory();
}

/** The server owns the list of transfers; WebSocket events only animate them. */
async function refreshTransfers() {
    let transfers;
    try {
        const res = await fetch("/api/transfers");
        if (!res.ok) return;
        transfers = await res.json();
    } catch {
        return;
    }

    const seen = new Set();
    for (const t of transfers) {
        // Tag the row this tab's own upload belongs to, so cancelTransfer()
        // knows which in-flight fetch to abort.
        if (sending && t.direction === "send" && !currentUpload.sessionId
            && (t.status.state === "pending" || t.status.state === "active")) {
            currentUpload.sessionId = t.session_id;
        }

        for (const f of t.files) {
            const key = rowKey(t.session_id, f.file_id);
            seen.add(key);
            const row = ensureRow(t.session_id, f.file_id, f.name, t.peer, t.direction);

            if (f.done || t.status.state !== "active") {
                const pct = f.size > 0 ? Math.min((f.bytes / f.size) * 100, 100) : 0;
                if (!row.done) row.fill.style.width = `${f.done ? 100 : pct}%`;

                if (f.error || t.status.state === "failed") {
                    markRowDone(row);
                    row.status.className = "transfer-status status-failed";
                    row.status.textContent = "Error";
                    row.detail.textContent = f.error || t.status.error || "Falló";
                    row.fill.classList.add("failed");
                } else if (t.status.state === "rejected") {
                    markRowDone(row);
                    row.status.className = "transfer-status status-failed";
                    row.status.textContent = "Rechazado";
                    row.detail.textContent = "El destinatario lo rechazó";
                    row.fill.classList.add("failed");
                } else if (t.status.state === "cancelled") {
                    markRowDone(row);
                    row.status.className = "transfer-status status-failed";
                    row.status.textContent = "Cancelado";
                    row.detail.textContent = "Transferencia cancelada";
                    row.fill.classList.add("failed");
                } else if (t.status.state === "pending") {
                    row.status.className = "transfer-status status-pending";
                    row.status.textContent = "Esperando aprobación";
                    row.detail.textContent = formatSize(f.size);
                } else if (f.done) {
                    markRowDone(row);
                    row.status.className = "transfer-status status-done";
                    row.status.textContent = "Completado";
                    row.detail.textContent = "100%";
                    row.fill.style.width = "100%";
                }
            }
        }
    }

    for (const [key, row] of rows) {
        if (!seen.has(key)) {
            row.item.remove();
            rows.delete(key);
            rates.delete(key);
        }
    }

    if (rows.size === 0 && !transferList.querySelector(".empty-state")) {
        transferList.replaceChildren(el("div", "empty-state", "Sin transferencias"));
    }
}

// ---------------------------------------------------------------- received files

async function refreshReceived() {
    let files;
    try {
        const res = await fetch("/api/received");
        if (!res.ok) return;
        files = await res.json();
    } catch {
        return;
    }

    if (files.length === 0) {
        receivedList.replaceChildren(el("div", "empty-state", "Aún no has recibido archivos"));
        return;
    }

    const frag = document.createDocumentFragment();
    files.slice(0, 20).forEach((f) => {
        const item = el("div", "received-item");
        item.appendChild(el("span", "received-icon", f.is_dir ? "📁" : "📄"));
        item.appendChild(el("span", "received-name", f.name));
        item.appendChild(el("span", "received-size", f.is_dir ? "carpeta" : formatSize(f.size)));
        frag.appendChild(item);
    });
    receivedList.replaceChildren(frag);
}

// ---------------------------------------------------------------- history (sqlite-backed)

function setupTabs() {
    const tabActive = $("tab-active");
    const tabHistory = $("tab-history");
    const viewActive = $("view-active");
    const viewHistory = $("view-history");

    tabActive.addEventListener("click", () => {
        tabActive.classList.add("active");
        tabActive.setAttribute("aria-selected", "true");
        tabHistory.classList.remove("active");
        tabHistory.setAttribute("aria-selected", "false");
        viewActive.classList.remove("hidden");
        viewHistory.classList.add("hidden");
    });

    tabHistory.addEventListener("click", () => {
        tabHistory.classList.add("active");
        tabHistory.setAttribute("aria-selected", "true");
        tabActive.classList.remove("active");
        tabActive.setAttribute("aria-selected", "false");
        viewHistory.classList.remove("hidden");
        viewActive.classList.add("hidden");
        refreshHistory();
    });

    $("clear-history-btn").addEventListener("click", async () => {
        if (!confirm("¿Borrar todo el historial de transferencias?")) return;
        await fetch("/api/history/clear", { method: "POST" });
        refreshHistory();
    });

    if (new URLSearchParams(location.search).get("tab") === "history") {
        tabHistory.click();
    }
}

const HISTORY_ICON = { send: "↗", recv: "↙" };
const HISTORY_LABEL = {
    completed: "Completado",
    rejected: "Rechazado",
    cancelled: "Cancelado",
    failed: "Error",
};

function formatWhen(epochSeconds) {
    const d = new Date(epochSeconds * 1000);
    const now = new Date();
    const sameDay = d.toDateString() === now.toDateString();
    const time = d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    return sameDay ? time : `${d.toLocaleDateString([], { day: "2-digit", month: "short" })} · ${time}`;
}

async function refreshHistory() {
    let entries;
    try {
        const res = await fetch("/api/history");
        if (!res.ok) return;
        entries = await res.json();
    } catch {
        return;
    }

    if (entries.length === 0) {
        historyList.replaceChildren(el("div", "empty-state", "Aún no hay historial"));
        return;
    }

    const frag = document.createDocumentFragment();
    entries.forEach((e) => {
        const item = el("div", "history-item");
        item.appendChild(el("span", "history-dir", HISTORY_ICON[e.direction] || "•"));

        const main = el("div", "history-main");
        main.appendChild(el("span", "history-peer", e.peer));
        const detail = `${e.file_count} archivo(s) · ${formatSize(e.total_size)}` + (e.error ? ` · ${e.error}` : "");
        main.appendChild(el("span", "history-detail", detail));
        item.appendChild(main);

        item.appendChild(el("span", `history-status ${e.status}`, HISTORY_LABEL[e.status] || e.status));
        item.appendChild(el("span", "history-when", formatWhen(e.finished_at)));
        frag.appendChild(item);
    });
    historyList.replaceChildren(frag);
}

// ---------------------------------------------------------------- peers

async function refreshPeers() {
    let peers;
    try {
        const res = await fetch("/api/peers");
        if (!res.ok) return;
        peers = await res.json();
    } catch {
        return;
    }

    if (peers.length === 0) {
        const empty = el("div", "empty-state", "No se encontraron dispositivos");
        empty.appendChild(el("div", "hint", "Ambos equipos deben estar en la misma red"));
        peerList.replaceChildren(empty);
        searchBadge.textContent = "Sin dispositivos";
        searchBadge.className = "status-badge offline";
        return;
    }

    searchBadge.textContent = `${peers.length} dispositivo(s)`;
    searchBadge.className = "status-badge online";

    // Keep the selection alive across refreshes.
    if (selectedPeer && !peers.some((p) => p.fingerprint === selectedPeer.fingerprint)) {
        selectedPeer = null;
        updateSendButton();
    }

    const frag = document.createDocumentFragment();
    peers.forEach((peer) => {
        const isSelected = selectedPeer?.fingerprint === peer.fingerprint;
        const div = el("div", "peer-item" + (isSelected ? " selected" : ""));

        const info = el("div", "peer-info");
        info.appendChild(el("div", "peer-status"));
        info.appendChild(el("span", "peer-name", peer.alias));

        div.appendChild(info);
        div.appendChild(el("span", "peer-details", peer.ip));
        div.addEventListener("click", () => {
            selectedPeer = peer;
            refreshPeers();
            updateSendButton();
        });
        frag.appendChild(div);
    });
    peerList.replaceChildren(frag);
}

// ---------------------------------------------------------------- file selection

/** Directory drops don't set webkitRelativePath, so carry the path ourselves. */
const relPathOf = (file) => file._rel || file.webkitRelativePath || null;

async function collectEntry(entry, files) {
    if (entry.isFile) {
        const file = await new Promise((resolve, reject) => entry.file(resolve, reject));
        const rel = entry.fullPath?.replace(/^\//, "");
        if (rel && rel.includes("/")) file._rel = rel;
        files.push(file);
    } else if (entry.isDirectory) {
        const reader = entry.createReader();
        // readEntries returns at most ~100 entries per call; loop until empty.
        for (;;) {
            const batch = await new Promise((resolve) => reader.readEntries(resolve, () => resolve([])));
            if (!batch.length) break;
            for (const child of batch) await collectEntry(child, files);
        }
    }
}

function setupDragAndDrop() {
    ["dragenter", "dragover", "dragleave", "drop"].forEach((name) => {
        dropZone.addEventListener(name, (e) => {
            e.preventDefault();
            e.stopPropagation();
        });
    });
    ["dragenter", "dragover"].forEach((n) => dropZone.addEventListener(n, () => dropZone.classList.add("drag-over")));
    ["dragleave", "drop"].forEach((n) => dropZone.addEventListener(n, () => dropZone.classList.remove("drag-over")));

    dropZone.addEventListener("drop", async (e) => {
        const entries = [...e.dataTransfer.items]
            .filter((i) => i.kind === "file")
            .map((i) => i.webkitGetAsEntry?.())
            .filter(Boolean);

        const files = [];
        for (const entry of entries) await collectEntry(entry, files);

        // Fall back to the plain file list if the entry API gave us nothing.
        showFilePreview(files.length ? files : [...e.dataTransfer.files]);
    });
}

function showFilePreview(files) {
    selectedFiles = files;
    if (files.length === 0) {
        filePreview.classList.add("hidden");
        updateSendButton();
        return;
    }

    const frag = document.createDocumentFragment();
    let total = 0;
    files.slice(0, 50).forEach((file) => {
        const li = el("li");
        li.appendChild(el("span", "file-name", relPathOf(file) || file.name));
        li.appendChild(el("span", "file-size", formatSize(file.size)));
        frag.appendChild(li);
    });
    files.forEach((f) => { total += f.size; });
    if (files.length > 50) frag.appendChild(el("li", "modal-more", `y ${files.length - 50} más...`));

    fileList.replaceChildren(frag);
    fileCount.textContent = `${files.length} archivo(s) · ${formatSize(total)}`;
    filePreview.classList.remove("hidden");
    updateSendButton();
}

function updateSendButton() {
    sendBtn.disabled = sending || !selectedPeer || selectedFiles.length === 0;
    if (sending) return;
    sendBtn.textContent = selectedPeer ? `Enviar a ${selectedPeer.alias}` : "Enviar";
}

// ---------------------------------------------------------------- sending

async function sendSelected() {
    if (sending || !selectedPeer || selectedFiles.length === 0) return;
    sending = true;
    sendBtn.disabled = true;
    sendBtn.textContent = "Esperando aprobación...";

    // Manifest first: the peer needs names and sizes to decide before any
    // bytes move. Field order must match the manifest order.
    const manifest = selectedFiles.map((f) => ({
        name: f.name,
        size: f.size,
        relative_path: relPathOf(f),
    }));

    const form = new FormData();
    form.append("manifest", JSON.stringify(manifest));
    selectedFiles.forEach((f, i) => form.append(`file${i}`, f, f.name));

    const url = `/api/send?target_ip=${encodeURIComponent(selectedPeer.ip)}`
        + `&target_tcp_port=${selectedPeer.tcp_port}`
        + `&target_alias=${encodeURIComponent(selectedPeer.alias)}`;

    // Filled in once refreshTransfers() spots this session, so a Cancel click
    // on its row can abort this exact fetch, not just tell the server to stop.
    currentUpload.sessionId = null;
    currentUpload.controller = new AbortController();

    try {
        const res = await fetch(url, { method: "POST", body: form, signal: currentUpload.controller.signal });
        const data = await res.json();

        if (data.status === "ok") {
            showToast(`${data.files} archivo(s) enviados`, "success");
            notify("Envío completado", `${data.files} archivo(s) → ${selectedPeer.alias}`);
            showFilePreview([]);
            fileInput.value = "";
            folderInput.value = "";
        } else {
            showToast(data.error || "Error al enviar", "error");
            notify("Envío fallido", data.error || "Error al enviar");
        }
    } catch (e) {
        if (e.name === "AbortError") {
            showToast("Envío cancelado", "info");
        } else {
            showToast("Error de red: " + e.message, "error");
        }
    } finally {
        sending = false;
        currentUpload.sessionId = null;
        currentUpload.controller = null;
        updateSendButton();
        refreshTransfers();
    }
}

// ---------------------------------------------------------------- manual connect

async function manualConnect() {
    const ip = manualIp.value.trim();
    if (!/^\d{1,3}(\.\d{1,3}){3}$/.test(ip)) {
        return showToast("Formato de IP inválido (ej: 192.168.1.100)", "error");
    }

    connectBtn.disabled = true;
    connectBtn.textContent = "Buscando...";
    try {
        const res = await fetch("/api/peers/connect", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ ip }),
        });
        const data = await res.json();
        if (data.status === "error") {
            showToast(data.error, "error");
        } else {
            // The peer answers our probe on the discovery port; poll briefly.
            for (let i = 0; i < 5; i++) {
                await sleep(800);
                await refreshPeers();
                if (peerList.querySelector(".peer-item")) {
                    showToast("¡Dispositivo encontrado!", "success");
                    manualIp.value = "";
                    return;
                }
            }
            showToast("Sin respuesta. Verifica la IP y el firewall.", "error");
        }
    } catch (e) {
        showToast("Error de red: " + e.message, "error");
    } finally {
        connectBtn.disabled = false;
        connectBtn.textContent = "Conectar";
    }
}

// ---------------------------------------------------------------- boot

document.addEventListener("DOMContentLoaded", async () => {
    setupDragAndDrop();
    setupNotifications();
    setupTabs();

    $("browse-btn").addEventListener("click", () => fileInput.click());
    $("browse-folder-btn").addEventListener("click", () => folderInput.click());
    fileInput.addEventListener("change", (e) => showFilePreview([...e.target.files]));
    folderInput.addEventListener("change", (e) => showFilePreview([...e.target.files]));

    sendBtn.addEventListener("click", sendSelected);
    $("cancel-btn").addEventListener("click", () => {
        showFilePreview([]);
        fileInput.value = "";
        folderInput.value = "";
    });

    connectBtn.addEventListener("click", manualConnect);
    manualIp.addEventListener("keydown", (e) => e.key === "Enter" && manualConnect());

    $("accept-btn").addEventListener("click", () => decide(true));
    $("reject-btn").addEventListener("click", () => decide(false));
    document.addEventListener("keydown", (e) => {
        if (e.key === "Escape" && !$("incoming-modal").classList.contains("hidden")) decide(false);
    });

    $("open-folder-btn").addEventListener("click", async () => {
        const res = await fetch("/api/received/open", { method: "POST" });
        const data = await res.json();
        if (data.status === "error") showToast(data.error, "error");
    });

    try {
        const info = await (await fetch("/api/state")).json();
        $("alias").textContent = info.alias;
        const pathEl = $("download-path");
        pathEl.textContent = info.download_dir;
        pathEl.title = `Los archivos recibidos se guardan en ${info.download_dir}`;
    } catch {
        // Non-fatal: the rest of the UI works without it.
    }

    setupWebSocket();
    await Promise.all([refreshPeers(), refreshTransfers(), refreshReceived()]);

    setInterval(refreshPeers, 3000);
    setInterval(refreshTransfers, 2000);
    setInterval(refreshReceived, 15000);
});
