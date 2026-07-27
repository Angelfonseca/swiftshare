#!/bin/bash
# Prueba de extremo a extremo con dos instancias reales en esta máquina.
# Envía archivos de PC-A a PC-B, aprueba desde PC-B y verifica integridad.

set -euo pipefail
cd "$(dirname "$0")"

TMP=$(mktemp -d)
PIDS=()
cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    rm -rf "$TMP"
}
trap cleanup EXIT

echo "== Compilando =="
cargo build --release --quiet

BIN=./target/release/swiftshare
mkdir -p "$TMP/inA" "$TMP/inB" "$TMP/origen/sub"

"$BIN" --alias PC-A --tcp-port 46001 --udp-port 46101 --http-port 8091 \
       --download-dir "$TMP/inA" > "$TMP/a.log" 2>&1 &
PIDS+=($!)
"$BIN" --alias PC-B --tcp-port 46002 --udp-port 46102 --http-port 8092 \
       --download-dir "$TMP/inB" > "$TMP/b.log" 2>&1 &
PIDS+=($!)

# Esperar a que ambas UIs respondan.
for _ in $(seq 1 50); do
    if curl -sf localhost:8091/api/state >/dev/null && curl -sf localhost:8092/api/state >/dev/null; then
        break
    fi
    sleep 0.2
done

echo "== Instancias arriba: PC-A :8091  PC-B :8092 =="

# Aprobador automático: hace lo que haría el usuario pulsando "Aceptar".
approve() {
    local decision=$1
    for _ in $(seq 1 200); do
        local sid
        sid=$(curl -s localhost:8092/api/transfers \
            | python3 -c "import sys,json;p=[t for t in json.load(sys.stdin) if t['status']['state']=='pending'];print(p[0]['session_id'] if p else '')")
        if [ -n "$sid" ]; then
            curl -s -X POST localhost:8092/api/decision \
                -H 'Content-Type: application/json' \
                -d "{\"session_id\":\"$sid\",\"accept\":$decision}" >/dev/null
            return
        fi
        sleep 0.1
    done
}

send() {
    curl -s -X POST "localhost:8091/api/send?target_ip=127.0.0.1&target_tcp_port=46002&target_alias=PC-B" \
        -F "manifest=$1" -F "file0=@$2"
}

head -c 50000000 /dev/urandom > "$TMP/origen/grande.bin"
echo "hola desde PC-A" > "$TMP/origen/sub/nota.txt"
NOTA_SIZE=$(wc -c < "$TMP/origen/sub/nota.txt" | tr -d ' ')

echo "== 1. Transferencia aceptada =="
approve true &
send "[{\"name\":\"grande.bin\",\"size\":50000000,\"relative_path\":null}]" "$TMP/origen/grande.bin"
echo
approve true &
send "[{\"name\":\"nota.txt\",\"size\":$NOTA_SIZE,\"relative_path\":\"sub/nota.txt\"}]" "$TMP/origen/sub/nota.txt"
echo

sleep 0.5
[ "$(shasum -a 256 < "$TMP/origen/grande.bin")" = "$(shasum -a 256 < "$TMP/inB/grande.bin")" ] \
    && echo "  OK: 50MB integros" || { echo "  FALLO: checksum distinto"; exit 1; }
diff -q "$TMP/origen/sub/nota.txt" "$TMP/inB/sub/nota.txt" >/dev/null \
    && echo "  OK: estructura de carpeta preservada" || { echo "  FALLO: ruta relativa"; exit 1; }

echo "== 2. Transferencia rechazada =="
approve false &
send "[{\"name\":\"secreto.txt\",\"size\":$NOTA_SIZE,\"relative_path\":null}]" "$TMP/origen/sub/nota.txt" | grep -q "rechaz" \
    && echo "  OK: el emisor recibe el rechazo" || { echo "  FALLO: no se reporto el rechazo"; exit 1; }
[ ! -f "$TMP/inB/secreto.txt" ] && echo "  OK: no se escribio nada" || { echo "  FALLO: se escribio pese al rechazo"; exit 1; }

echo "== 3. Ruta maliciosa =="
send "[{\"name\":\"pwned\",\"size\":$NOTA_SIZE,\"relative_path\":\"../../../pwned\"}]" "$TMP/origen/sub/nota.txt" | grep -q "no permitida" \
    && echo "  OK: traversal bloqueado antes de preguntar" || { echo "  FALLO: traversal aceptado"; exit 1; }
[ ! -e "$TMP/pwned" ] && echo "  OK: nada escapo de la carpeta" || { echo "  FALLO: escritura fuera del destino"; exit 1; }

echo "== 4. Colision de nombres =="
approve true &
send "[{\"name\":\"nota.txt\",\"size\":$NOTA_SIZE,\"relative_path\":null}]" "$TMP/origen/sub/nota.txt" >/dev/null
approve true &
send "[{\"name\":\"nota.txt\",\"size\":$NOTA_SIZE,\"relative_path\":null}]" "$TMP/origen/sub/nota.txt" >/dev/null
sleep 0.5
[ -f "$TMP/inB/nota (1).txt" ] && echo "  OK: no se sobrescribe" || { echo "  FALLO: archivo sobrescrito"; exit 1; }

echo
echo "== Todo correcto =="
