#!/bin/bash
# Script para probar swiftshare con 2 instancias en la misma máquina

set -e

cd "$(dirname "$0")"

echo "========================================="
echo "  swiftshare - Prueba de Transferencia"
echo "========================================="
echo ""

# Instancia 1: Puerto TCP 45678, HTTP 8080
echo "[1] Iniciando Instancia 1 (Puerto TCP 45678, Web 8080)..."
cargo run -- --alias "PC-A" --tcp-port 45678 --http-port 8080 2>&1 &
PID1=$!
sleep 2

# Instancia 2: Puerto TCP 45679, HTTP 8081
echo "[2] Iniciando Instancia 2 (Puerto TCP 45679, Web 8081)..."
cargo run -- --alias "PC-B" --tcp-port 45679 --http-port 8081 2>&1 &
PID2=$!
sleep 2

echo ""
echo "========================================="
echo "  Ambas instancias corriendo:"
echo "  PC-A: http://localhost:8080 (TCP 45678)"
echo "  PC-B: http://localhost:8081 (TCP 45679)"
echo "========================================="
echo ""

# Verificar que ambas responden
echo "--- Verificando PC-A ---"
curl -s http://localhost:8080/ | head -1
echo ""

echo "--- Verificando PC-B ---"
curl -s http://localhost:8081/ | head -1
echo ""

# Probar upload en PC-A
echo "--- Probando upload en PC-A ---"
echo "Hola desde PC-A" > /tmp/prueba.txt
curl -s -F "files=@/tmp/prueba.txt" http://localhost:8080/api/send
echo ""

echo "--- Listando archivos en PC-A ---"
curl -s http://localhost:8080/api/files/list
echo ""

# Probar upload en PC-B
echo "--- Probando upload en PC-B ---"
echo "Hola desde PC-B" > /tmp/prueba2.txt
curl -s -F "files=@/tmp/prueba2.txt" http://localhost:8081/api/send
echo ""

echo "--- Listando archivos en PC-B ---"
curl -s http://localhost:8081/api/files/list
echo ""

echo "========================================="
echo "  Prueba completada!"
echo "========================================="
echo ""
echo "Para abrir las UIs en el navegador:"
echo "  PC-A: open http://localhost:8080"
echo "  PC-B: open http://localhost:8081"
echo ""
echo "Para detener: kill $PID1 $PID2"

# Mantener corriendo hasta que el usuario pulse Ctrl+C
wait
