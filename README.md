# ms-barriodigital-kafkaadmin

Microservicio **administrador de Kafka**: crea, modifica y elimina tópicos y sus
configuraciones, e inspecciona consumer groups y su **lag**. Spring Boot 3.3.5 / Java 17,
puerto **8087**, **sin base de datos** (plano de control, stateless). Es el equivalente
para Kafka de [`ms-barriodigital-rabbitadmin`](https://github.com/PhamNukz/ms-barriodigital-rabbitadmin),
y sigue su mismo molde: seguridad JWT de Entra ID, `ProblemDetail`, DTO validados, CI y Dockerfile.

Matriz de cumplimiento:
[barriodigital-docs/Cumplimiento-Prueba3.md](https://github.com/PhamNukz/barriodigital-docs/blob/main/Cumplimiento-Prueba3.md).

## ¿Para qué, si existe Kafka UI?

Kafka UI sirve para operar a mano. Este servicio deja la administración **versionada,
validada, autorizada por rol y repetible**: los tópicos del sistema se crean con la
configuración correcta en cada ambiente sin depender de clics, y cualquier cambio pasa por
la misma validación y el mismo control de acceso.

## Tópicos del sistema (declarados como código)

`config/TopicsConfig.java` declara un `NewTopic` (`TopicBuilder`) por cada bloque de
`barriodigital.kafka.topics.*` en `application.yml`. Al arrancar, `KafkaAdmin` crea los que
faltan y, con `spring.kafka.admin.modify-topic-configs: true`, **alinea las configs de los
que ya existen** (`requests.events` hoy existe porque el broker lo auto-creó con sus defaults).

| Tópico | Part. | RF | `cleanup.policy` | `retention.ms` | `min.insync.replicas` | Por qué |
|---|---|---|---|---|---|---|
| `requests.events` | 3 | 3 | `delete` | 604800000 (7 d) | 2 | Hechos del trámite: KPIs y timeline. 7 d permiten un replay para recalcular KPIs |
| `audit.timeline` | 3 | 3 | `compact,delete` | 2592000000 (30 d) | 2 | Quién/qué/cuándo. Key = `eventId`: compact colapsa reenvíos del mismo evento sin borrar la historia; la historia larga queda en Oracle |
| `requests.events.DLT` | 3 | 3 | `delete` | 1209600000 (14 d) | 2 | Mensajes que fallaron tras los reintentos. 3 particiones porque el `DeadLetterPublishingRecoverer` publica en la misma partición del original |
| `audit.timeline.DLT` | 3 | 3 | `delete` | 1209600000 (14 d) | 2 | Ídem |

3 particiones = hasta 3 consumidores en paralelo por grupo. RF 3 con `min.insync.replicas` 2
y `acks=all` = el clúster tolera la caída de 1 broker sin perder escrituras.

### La regla: la topología base es código, lo operativo es API

Igual que en rabbitadmin: estos 4 tópicos se **re-aplican en cada arranque**. Un `PATCH` a
uno de ellos dura hasta que este servicio reinicie; el cambio permanente se hace en
`application.yml` y se versiona. La API sirve para crear tópicos nuevos, inspeccionar,
ajustar en caliente y reparar.

## API

Todo `/admin/**` exige rol **Admin** (JWT de Entra ID, claim `roles`). La regla está en un
solo lugar, `config/SecurityConfig.java`. `service/KafkaAdminService.java` es la única clase
que usa el `AdminClient`.

| Método | Ruta | Qué hace | API de Kafka | CLI equivalente |
|---|---|---|---|---|
| `GET` | `/admin/cluster` | Id del clúster, controller y brokers | `describeCluster` | `kafka-broker-api-versions.sh --bootstrap-server kafka1:9092` |
| `GET` | `/admin/topics` | Tópicos con particiones y RF (sin los internos) | `listTopics` + `describeTopics` | `kafka-topics.sh --bootstrap-server kafka1:9092 --list` |
| `GET` | `/admin/topics/{nombre}` | Líder, réplicas e ISR por partición, y configs (valor y origen) | `describeTopics` + `describeConfigs` | `kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic requests.events` y `kafka-configs.sh --bootstrap-server kafka1:9092 --entity-type topics --entity-name requests.events --describe` |
| `POST` | `/admin/topics` | Crea un tópico | `createTopics` | `kafka-topics.sh --bootstrap-server kafka1:9092 --create --topic pagos.events --partitions 3 --replication-factor 3 --config retention.ms=604800000 --config min.insync.replicas=2` |
| `PATCH` | `/admin/topics/{nombre}/config` | Modifica configs; un valor `null` vuelve al default del broker | `incrementalAlterConfigs` | `kafka-configs.sh --bootstrap-server kafka1:9092 --entity-type topics --entity-name requests.events --alter --add-config retention.ms=86400000` (`--delete-config` para quitar) |
| `POST` | `/admin/topics/{nombre}/partitions` | Sube el total de particiones (Kafka no permite bajarlas) | `createPartitions` | `kafka-topics.sh --bootstrap-server kafka1:9092 --alter --topic requests.events --partitions 6` |
| `DELETE` | `/admin/topics/{nombre}` | Elimina el tópico | `deleteTopics` | `kafka-topics.sh --bootstrap-server kafka1:9092 --delete --topic pagos.events` |
| `GET` | `/admin/consumer-groups` | Grupos y su estado | `listConsumerGroups` | `kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --list` |
| `GET` | `/admin/consumer-groups/{id}` | Por partición: consumidor asignado, offset comprometido, end offset y **lag**; más el lag total | `describeConsumerGroups` + `listConsumerGroupOffsets` + `listOffsets` | `kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --describe --group report-svc` |

La CLI se corre dentro de un broker: `docker exec -it barriodigital-kafka-kafka1-1 bash`. En
la imagen Bitnami los scripts terminan en `.sh`.

### Cuerpos

```jsonc
// POST /admin/topics   (configs es opcional)
{"nombre": "pagos.events", "particiones": 3, "replicas": 3,
 "configs": {"retention.ms": "604800000", "cleanup.policy": "delete", "min.insync.replicas": "2"}}

// PATCH /admin/topics/requests.events/config
{"configs": {"retention.ms": "86400000", "cleanup.policy": null}}

// POST /admin/topics/requests.events/partitions   (total nuevo, no cuántas agregar)
{"particiones": 6}
```

### Lag

```text
lag(partición) = endOffset (listOffsets latest) − offset comprometido del grupo (listConsumerGroupOffsets)
```

```jsonc
// GET /admin/consumer-groups/report-svc
{"id": "report-svc", "estado": "STABLE", "miembros": 3, "lagTotal": 5,
 "particiones": [
   {"topico": "requests.events", "particion": 0, "consumidor": "consumer-report-svc-1",
    "offsetComprometido": 40, "endOffset": 42, "lag": 2}, ...]}
```

Una partición asignada que todavía no tiene commit sale con `offsetComprometido` y `lag` en
`null`. Lag cerca de 0 = el consumidor va al día; lag creciente = está caído o es más lento
que el productor.

## Validación y errores

Todo sale como `ProblemDetail` con un `detail` legible:

| Código | Cuándo |
|---|---|
| `400` | Nombre de tópico vacío, con caracteres fuera de `[A-Za-z0-9._-]`, de más de 249 caracteres o que empieza con `__` (tópicos internos como `__consumer_offsets`), también en la ruta; `particiones` < 1; `replicas` fuera de 1–3 (el clúster tiene 3 brokers); config fuera de la lista blanca (`retention.ms`, `retention.bytes`, `cleanup.policy`, `min.insync.replicas`, `max.message.bytes`); `min.insync.replicas` mayor que las réplicas (ninguna escritura con `acks=all` podría completarse); y lo que Kafka rechaza por entrada inválida (valor de config inválido, bajar particiones) |
| `404` | El tópico o el consumer group no existen |
| `409` | Crear un tópico que ya existe (`TopicExistsException`) |
| `502` | El broker no respondió (timeout de 10 s, `default.api.timeout.ms`) o falló por otra causa |

Validaciones de forma en `web/AdminDtos.java`; lista blanca y `min.insync` en
`service/KafkaAdminService.java`; traducción a HTTP en `web/ApiExceptionHandler.java`.

## Correr y probar

```bash
mvn -B verify
```

Los tests **no necesitan Kafka**: el `AdminClient` se mockea. `KafkaAdminServiceTest` prueba
el servicio (lo que se envía al broker, la lista blanca, el cálculo del lag, el desenvolvimiento
de errores) y `AdminApiTest` la API con MockMvc (`401`, `403`, `201`, `204`, `400`, `404`,
`409`, `502`) y que los 4 tópicos del yml se declaren con sus configs.

Variables de entorno: `KAFKA_BOOTSTRAP_SERVERS` (en AWS, los 3 brokers de `ec2-kafka`:
`IP:9092,IP:9093,IP:9094`), más las de Entra (`AAD_ISSUER_URI`, `AAD_API_CLIENT_ID`,
`AAD_REQUIRED_SCOPE`).

Una vez que este servicio haya creado los tópicos, el broker puede pasar a
`KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "false"` (en `barriodigital-infra/kafka/compose.yml`):
así ningún tópico nace con los defaults por un error de tipeo en un productor.
