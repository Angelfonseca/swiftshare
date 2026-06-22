# swiftshare - Releases

## Binarios precompilados

### macOS ARM64 (Apple Silicon - M1/M2/M3/M4)
```bash
curl -L https://github.com/tu-usuario/swiftshare/releases/latest/download/swiftshare-macos-arm64 -o swiftshare
chmod +x swiftshare
./swiftshare
```

### macOS Intel
```bash
# Compilar desde fuente
cargo install --path .
swiftshare
```

### Linux x86_64
```bash
# Compilar desde fuente
cargo install --path .
swiftshare
```

### Windows
```powershell
# Compilar desde fuente (necesitas Rust instalado)
cargo build --release
.\target\release\swiftshare.exe
```

## Uso
```bash
# Iniciar
./swiftshare

# Con alias personalizado
./swiftshare --alias "MiPC"

# Puertos personalizados
./swiftshare --tcp-port 45678 --udp-port 45679 --http-port 8080

# Ver ayuda
./swiftshare --help
```

## Compilar desde fuente
```bash
git clone <tu-repo>
cd swiftshare
cargo build --release
```
