# TFC-WS

Servicio backend construido con Spring Boot 3 y WebFlux para exponer un WebSocket reactivo y un conjunto mínimo de endpoints REST enfocados en monitorización.

## Características principales
- **Stack reactivo** con Spring WebFlux y Netty listo para manejar conexiones concurrentes.
- **Endpoint WebSocket** (`/ws/game`) que actúa como servicio de eco y registra el ciclo de vida de cada sesión.
- **Endpoint REST de salud** (`GET /health`) para integraciones con sondas de disponibilidad.
- **Registros de ciclo de vida** en el arranque y apagado de la aplicación a través de `LifeCycleLogs`.
- **Integración con PostgreSQL** mediante configuración externa y un `compose.yaml` que levanta una base de datos y Adminer para pruebas locales.

## Requisitos previos
- Java 21.
- Maven 3.9+ (se incluye `mvnw`/`mvnw.cmd` para no depender de una instalación global).
- Docker y Docker Compose (opcional, solo si deseas levantar la base de datos local definida en `compose.yaml`).

## Configuración
La configuración principal reside en `src/main/resources/application.properties`. Las propiedades relevantes se leen desde variables de entorno, por ejemplo:

```properties
spring.application.name=${APP_NAME}
app.env=${APP_ENV}
spring.datasource.url=${DATA_SOURCE_URL}
```

Puedes definirlas en tu shell o en un archivo `.env` cuando ejecutes `docker compose`.

## Puesta en marcha local

```bash
./mvnw spring-boot:run
```

El servicio quedará disponible en `http://localhost:8080` (puerto configurable con la propiedad `server.port`).

### Probar el WebSocket
Puedes utilizar [`wscat`](https://github.com/websockets/wscat) u otra herramienta WebSocket para verificar el comportamiento de eco:

```bash
wscat -c ws://localhost:8080/ws/game
> Hola
< Echo: Hola
```

### Health check

```bash
curl http://localhost:8080/health
# {"status":"UP"}
```

## Base de datos opcional
Para levantar PostgreSQL y Adminer según el archivo `compose.yaml`:

```bash
docker compose up -d
```

Asegúrate de definir las variables `POSTGRES_USER`, `POSTGRES_PASSWORD` y `POSTGRES_DB`. La base de datos expone el puerto `5433` en tu máquina local y Adminer queda disponible en `http://localhost:8081`.

## Ejecución de pruebas

```bash
./mvnw test
```

Esto ejecuta las pruebas unitarias básicas que validan el arranque del contexto Spring Boot.

## Estructura del proyecto

```
├── compose.yaml                 # Definición de servicios auxiliares (PostgreSQL, Adminer)
├── src
│   ├── main
│   │   ├── java
│   │   │   └── org/jpsoft/tfcws
│   │   │       ├── TfcWsApplication.java       # Clase principal Spring Boot
│   │   │       ├── app/LifeCycleLogs.java      # Logs en arranque/cierre
│   │   │       ├── web/controller/HealthController.java  # Endpoint /health
│   │   │       └── ws
│   │   │           ├── WsConfig.java           # Configuración de rutas WebSocket
│   │   │           └── WsHandler.java          # Lógica del WebSocket de eco
│   │   └── resources/application.properties   # Configuración externa
│   └── test/java/org/jpsoft/tfcws
│       └── TfcWsApplicationTests.java         # Smoke test del contexto
└── pom.xml
```

## Desarrollo y futuras mejoras
- Ajustar la configuración de `docker compose` para entornos de producción (ocultar puertos públicos, definir volúmenes externos, automatizar copias de seguridad).
- Implementar lógica de negocio en `WsHandler` más allá del eco, por ejemplo gestión de salas o validación de mensajes.
- Añadir pruebas unitarias e integrales para la capa WebSocket y REST.

## Licencia
No se ha definido una licencia explícita. Añádela según las necesidades del proyecto.
