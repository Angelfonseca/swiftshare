# swiftshare

Transferencia de archivos P2P para red local, escrita en Rust.

## Características

- **P2P Simétrico**: Ambas PCs son iguales, cualquiera puede enviar o recibir
- **Descubrimiento automático**: UDP broadcast + multicast para encontrar peers
- **Conexión manual**: Conectar por IP si el auto-descubrimiento falla
- **Transferencia rápida**: TCP con chunks de 64KB y streaming directo a disco
- **Verificación SHA-256**: Integridad garantizada en cada archivo
- **Web UI**: Interfaz moderna, drag & drop, progreso en tiempo real
- **Resumen de transferencias**: Reanudar transferencias interrumpidas

## Instalación

```bash
cargo install --path .
```

## Uso

```bash
# Iniciar servidor
swiftshare

# Con alias personalizado
swiftshare --alias "MiPC"

# Puertos personalizados
swiftshare --tcp-port 45678 --udp-port 45679 --http-port 8080

# Directorio de descarga
swiftshare --download-dir ~/Downloads

# Ver ayuda
swiftshare --help
```

## Arquitectura

- **UDP 45679**: Descubrimiento de peers (broadcast + multicast)
- **TCP 45678**: Transferencia de archivos
- **HTTP 8080**: Web UI (localhost)

## Estructura del proyecto

```
swiftshare/
├── src/
│   ├── main.rs          # Entry point
│   ├── cli.rs           # CLI parsing
│   ├── protocol.rs      # Protocolo TCP
│   ├── codec.rs         # Framing TCP
│   ├── state.rs         # Estado compartido
│   ├── server.rs        # Web UI server
│   ├── discovery.rs     # UDP discovery
│   ├── transfer.rs      # Transferencia TCP
│   └── resume.rs        # Reanudar transferencias
└── web/
    ├── index.html       # UI web
    ├── styles.css       # Estilos
    └── app.js           # JavaScript
```

## Tests

```bash
cargo test
```
