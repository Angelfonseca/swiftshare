# swiftshare

Transferencia de archivos P2P para red local, escrita en Rust.

## Características

- **P2P simétrico**: ambos equipos son iguales, cualquiera puede enviar o recibir
- **Aprobación explícita**: nada se escribe en disco hasta que aceptas la transferencia
- **Notificaciones**: aviso del sistema + sonido cuando alguien quiere enviarte algo
- **Descubrimiento automático**: broadcast + multicast UDP, con respuesta unicast
  para redes donde el broadcast está filtrado
- **Conexión manual por IP**: si el descubrimiento falla
- **Streaming directo**: los bytes van del navegador al peer sin pasar por disco
- **Verificación SHA-256**: se calcula al vuelo; un archivo corrupto se descarta
- **Carpetas completas**: se preserva la estructura de directorios
- **Progreso en tiempo real**: velocidad y ETA por archivo vía WebSocket

## Instalación

```bash
cargo install --path .
```

## Uso

```bash
swiftshare                       # alias aleatorio, UI en http://localhost:8080
swiftshare --open                # además abre el navegador
swiftshare --alias "MiPC"
swiftshare --download-dir ~/Recibidos
swiftshare --tcp-port 45678 --udp-port 45679 --http-port 8080
swiftshare --help
```

Ejecuta swiftshare en ambos equipos, abre la UI en cualquiera de los dos,
selecciona el dispositivo destino y arrastra archivos o carpetas. En el equipo
receptor aparece un aviso para aceptar o rechazar.

Los archivos recibidos van a `~/Downloads/swiftshare` salvo que uses `--download-dir`.

## Arquitectura

- **UDP 45679**: descubrimiento de peers
- **TCP 45678**: transferencia de archivos
- **HTTP 8080**: Web UI

### Protocolo

Tramas `[u8 tipo][u32 be longitud][carga]`, donde el tipo distingue comando JSON
de bytes crudos.

```
PrepareTransfer  ->                el emisor anuncia el lote (nombres y tamaños)
            <-  TransferResponse   el receptor acepta o rechaza tras preguntar
StartFile / Data* / FileComplete   un archivo, con su SHA-256 al final
SessionComplete
```

El receptor resuelve todas las rutas destino antes de preguntar: si alguna
intenta salirse de la carpeta de descargas, rechaza el lote entero. Los datos se
escriben en `.part` y solo se renombran cuando el checksum cuadra.

## Estructura

```
src/
├── main.rs        # Entry point
├── cli.rs         # CLI parsing
├── protocol.rs    # Comandos del protocolo
├── codec.rs       # Framing TCP
├── state.rs       # Estado compartido, eventos y cola de aprobaciones
├── server.rs      # Web UI + API HTTP/WebSocket
├── discovery.rs   # Descubrimiento UDP
└── transfer.rs    # Recepción y envío TCP
web/
├── index.html
├── styles.css
└── app.js
```

## Tests

```bash
cargo test     # unitarios + extremo a extremo sobre sockets reales
./test.sh      # dos instancias reales: aceptar, rechazar, traversal, colisiones
```
