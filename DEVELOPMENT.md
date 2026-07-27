# swiftshare - Desarrollo

## Notas de diseño

### Camino de envío

El navegador manda una sola petición multipart a `/api/send`. El primer campo es
un **manifiesto** JSON con nombres, tamaños y rutas relativas; los siguientes son
los archivos, en ese mismo orden.

El servidor abre la conexión TCP al peer, envía `PrepareTransfer` con el
manifiesto y **espera la aprobación** antes de leer un solo byte de archivo. A
partir de ahí los bytes van del multipart al socket TCP directamente, calculando
el SHA-256 de paso. La respuesta HTTP no vuelve hasta que el peer tiene todo.

Esto evita las tres pasadas de la versión anterior (guardar en temporal, releer
para hashear, releer para enviar) y el buffer completo en disco antes de mover
nada. La contrapresión sale gratis: el multipart se lee al ritmo del socket.

### Aprobación

`AppState::await_decision` registra un `oneshot` por sesión y bloquea el
handler de recepción. `POST /api/decision` lo resuelve. Si nadie contesta en
`DECISION_TIMEOUT` (120 s) se trata como rechazo — nunca se escriben archivos
que nadie aprobó. El emisor espera 15 s más que eso antes de rendirse.

### Eventos

Un único `broadcast::Sender<Event>` alimenta el WebSocket. Los eventos van
etiquetados (`incoming`, `decided`, `progress`, `fileDone`, `sessionDone`) y la
UI conmuta sobre `type`. El progreso se limita a uno cada 100 ms por archivo
para no inundar la conexión en transferencias grandes.

La lista de transferencias la sirve `/api/transfers`: el servidor es la fuente
de verdad de qué filas existen, y el WebSocket solo las anima. Así un refresco
de página recupera el estado.

### Seguridad

Todo lo que llega del peer es hostil por defecto:

- `safe_join` rechaza `..`, prefijos de unidad y NUL, y recorta puntos finales
  (Windows los ignora, lo que permitiría que el nombre aprobado y el escrito
  difieran). Las rutas se resuelven **antes** de preguntar al usuario: si alguna
  se sale, se rechaza el lote entero.
- Los datos se escriben en `.part` y solo se renombran si el SHA-256 cuadra; si
  no, se borran.
- Los nombres existentes no se sobrescriben (`nota.txt` → `nota (1).txt`).
- El alias del peer se limpia de caracteres de control y se acota a 64 chars.
- La UI construye todo con `textContent`; nada que venga de la red pasa por
  `innerHTML`.

### Framing

`[u8 tipo][u32 be longitud][carga]`. El tipo importa: el códec anterior
adivinaba intentando parsear cada trama como JSON, lo que costaba un intento de
parseo sobre cada megabyte de datos y podía malinterpretar binario con forma de
comando.

## Pendiente

- Reanudar transferencias interrumpidas (`ResumeRequest` se quitó del protocolo
  al no estar implementado; el `.part` ya deja la base para volver a añadirlo).
- Cancelar una transferencia en curso desde la UI (hoy solo se puede rechazar
  antes de empezar).
- Cifrado. El tráfico va en claro; sirve para redes locales de confianza.

## Verificación

```bash
cargo test              # 25 tests: unitarios + e2e sobre sockets reales
./test.sh               # dos instancias reales, 4 escenarios
cargo build --release
```
