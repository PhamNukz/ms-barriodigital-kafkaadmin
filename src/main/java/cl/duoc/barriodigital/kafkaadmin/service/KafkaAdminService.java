package cl.duoc.barriodigital.kafkaadmin.service;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.TopicConfig.*;

/**
 * Unico punto del servicio que conoce el {@link AdminClient}. Los controladores
 * hablan de nombres, particiones y configs; los records de abajo son lo unico que
 * sale hacia la capa web, sin tipos de Kafka.
 *
 * <p>Los errores del broker se propagan tal cual (excepciones de
 * {@code org.apache.kafka.common.errors}) y {@code ApiExceptionHandler} los
 * traduce a 400/404/409/502. Los timeouts los pone el cliente
 * ({@code default.api.timeout.ms} en application.yml), no este codigo.
 *
 * <p><b>Frontera con la topologia declarada.</b> Los 4 topicos del sistema son
 * codigo ({@code TopicsConfig}) y se re-aplican al arrancar; esta API los muta en
 * runtime. Un {@code PATCH} a {@code requests.events} dura hasta el proximo
 * reinicio, igual que en rabbitadmin: la topologia base es codigo, lo operativo es API.
 */
@Service
public class KafkaAdminService {

    /**
     * Lo que la API deja fijar o modificar. Lo demas (segment.bytes, compression,
     * etc.) queda con el valor del broker: cambiarlo a mano rompe mas de lo que arregla.
     */
    public static final Set<String> CONFIGS_PERMITIDAS = Set.of(
            RETENTION_MS_CONFIG, RETENTION_BYTES_CONFIG, CLEANUP_POLICY_CONFIG,
            MIN_IN_SYNC_REPLICAS_CONFIG, MAX_MESSAGE_BYTES_CONFIG);

    private final AdminClient admin;

    public KafkaAdminService(AdminClient admin) {
        this.admin = admin;
    }

    public record Broker(int id, String host, int puerto, String rack) {
    }

    public record Cluster(String id, Integer controller, List<Broker> brokers) {
    }

    public record TopicoResumen(String nombre, int particiones, int replicas) {
    }

    /** {@code lider} es null si la particion no tiene lider (todas sus replicas caidas). */
    public record Particion(int particion, Integer lider, List<Integer> replicas, List<Integer> isr) {
    }

    /** {@code origen}: DYNAMIC_TOPIC_CONFIG si se fijo en el topico; si no, viene del broker o del default. */
    public record ConfigValor(String valor, String origen) {
    }

    public record TopicoDetalle(String nombre, boolean interno, List<Particion> particiones,
                                Map<String, ConfigValor> configs) {
    }

    public record GrupoResumen(String id, String estado) {
    }

    /** {@code offsetComprometido} y {@code lag} son null si el grupo aun no hizo commit en esa particion. */
    public record LagParticion(String topico, int particion, String consumidor,
                               Long offsetComprometido, long endOffset, Long lag) {
    }

    public record GrupoDetalle(String id, String estado, int miembros, long lagTotal,
                               List<LagParticion> particiones) {
    }

    // ---------- cluster ----------

    public Cluster cluster() {
        DescribeClusterResult r = admin.describeCluster();
        Node controller = esperar(r.controller());
        List<Broker> brokers = esperar(r.nodes()).stream()
                .sorted(Comparator.comparingInt(Node::id))
                .map(n -> new Broker(n.id(), n.host(), n.port(), n.rack()))
                .toList();
        return new Cluster(esperar(r.clusterId()), controller == null ? null : controller.id(), brokers);
    }

    // ---------- topicos ----------

    /** Sin los internos ({@code __consumer_offsets}): {@code listTopics()} los omite por defecto. */
    public List<TopicoResumen> listarTopicos() {
        Set<String> nombres = esperar(admin.listTopics().names());
        if (nombres.isEmpty()) {
            return List.of();
        }
        return esperar(admin.describeTopics(nombres).allTopicNames()).values().stream()
                .sorted(Comparator.comparing(TopicDescription::name))
                .map(t -> new TopicoResumen(t.name(), t.partitions().size(),
                        t.partitions().isEmpty() ? 0 : t.partitions().get(0).replicas().size()))
                .toList();
    }

    /** Particiones con lider, replicas e ISR, y las configs editables mas las fijadas en el topico. */
    public TopicoDetalle detalleTopico(String nombre) {
        TopicDescription t = esperarTopico(nombre, admin.describeTopics(List.of(nombre)).allTopicNames()).get(nombre);
        ConfigResource recurso = topico(nombre);
        Config config = esperarTopico(nombre, admin.describeConfigs(List.of(recurso)).all()).get(recurso);

        List<Particion> particiones = t.partitions().stream()
                .map(p -> new Particion(p.partition(), p.leader() == null ? null : p.leader().id(),
                        ids(p.replicas()), ids(p.isr())))
                .toList();
        Map<String, ConfigValor> configs = new TreeMap<>();
        config.entries().stream()
                .filter(e -> CONFIGS_PERMITIDAS.contains(e.name())
                        || e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
                .forEach(e -> configs.put(e.name(), new ConfigValor(e.value(), e.source().name())));
        return new TopicoDetalle(t.name(), t.isInternal(), particiones, configs);
    }

    public void crearTopico(String nombre, int particiones, int replicas, Map<String, String> configs) {
        Map<String, String> cfg = configs == null ? Map.of() : configs;
        validarConfigs(cfg.keySet());
        validarMinInsync(cfg.get(MIN_IN_SYNC_REPLICAS_CONFIG), replicas);
        esperar(admin.createTopics(List.of(
                new NewTopic(nombre, particiones, (short) replicas).configs(cfg))).all());
    }

    /**
     * {@code incrementalAlterConfigs}: solo toca las claves que vienen. Valor null =
     * quitar el override del topico y volver al default del broker.
     */
    public void modificarConfig(String nombre, Map<String, String> cambios) {
        validarConfigs(cambios.keySet());
        String minInsync = cambios.get(MIN_IN_SYNC_REPLICAS_CONFIG);
        if (minInsync != null) {
            validarMinInsync(minInsync, replicasDe(nombre));
        }
        List<AlterConfigOp> ops = cambios.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()),
                        e.getValue() == null ? AlterConfigOp.OpType.DELETE : AlterConfigOp.OpType.SET))
                .toList();
        esperarTopico(nombre, admin.incrementalAlterConfigs(Map.of(topico(nombre), ops)).all());
    }

    /** Kafka solo deja aumentar: pedir menos de las que hay vuelve como InvalidPartitionsException (400). */
    public void aumentarParticiones(String nombre, int total) {
        esperarTopico(nombre, admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(total))).all());
    }

    public void eliminarTopico(String nombre) {
        esperarTopico(nombre, admin.deleteTopics(List.of(nombre)).all());
    }

    // ---------- consumer groups ----------

    public List<GrupoResumen> listarGrupos() {
        return esperar(admin.listConsumerGroups().all()).stream()
                .sorted(Comparator.comparing(ConsumerGroupListing::groupId))
                .map(g -> new GrupoResumen(g.groupId(), g.state().map(Enum::name).orElse("UNKNOWN")))
                .toList();
    }

    /**
     * lag(particion) = endOffset (listOffsets latest) − offset comprometido del grupo
     * (listConsumerGroupOffsets). Se listan las particiones con commit y las
     * asignadas a un miembro aunque aun no tengan commit.
     */
    public GrupoDetalle detalleGrupo(String id) {
        ConsumerGroupDescription g = esperar(admin.describeConsumerGroups(List.of(id)).all()).get(id);
        // Kafka 3.7 no lanza error por un grupo desconocido: lo describe como DEAD y sin miembros.
        if (g == null || g.state() == ConsumerGroupState.DEAD) {
            throw new NoSuchElementException("No existe el consumer group " + id);
        }
        Map<TopicPartition, OffsetAndMetadata> comprometidos =
                esperar(admin.listConsumerGroupOffsets(id).partitionsToOffsetAndMetadata());

        Map<TopicPartition, String> asignadas = new HashMap<>();
        g.members().forEach(m -> m.assignment().topicPartitions().forEach(tp -> asignadas.put(tp, m.clientId())));

        Set<TopicPartition> todas = new HashSet<>(comprometidos.keySet());
        todas.addAll(asignadas.keySet());
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> fin = todas.isEmpty() ? Map.of()
                : esperar(admin.listOffsets(todas.stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()))).all());

        List<LagParticion> particiones = todas.stream()
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .map(tp -> {
                    OffsetAndMetadata c = comprometidos.get(tp);
                    long end = fin.get(tp).offset();
                    Long comprometido = c == null ? null : c.offset();
                    Long lag = comprometido == null ? null : Math.max(0, end - comprometido);
                    return new LagParticion(tp.topic(), tp.partition(), asignadas.get(tp), comprometido, end, lag);
                })
                .toList();
        long lagTotal = particiones.stream().map(LagParticion::lag).filter(Objects::nonNull)
                .mapToLong(Long::longValue).sum();
        return new GrupoDetalle(id, g.state().name(), g.members().size(), lagTotal, particiones);
    }

    // ---------- interno ----------

    private static void validarConfigs(Set<String> nombres) {
        List<String> rechazadas = nombres.stream().filter(n -> !CONFIGS_PERMITIDAS.contains(n)).sorted().toList();
        if (!rechazadas.isEmpty()) {
            throw new IllegalArgumentException("Config no permitida: " + rechazadas
                    + ". Se permiten: " + new TreeSet<>(CONFIGS_PERMITIDAS));
        }
    }

    /**
     * Kafka acepta min.insync.replicas mayor que el RF, pero entonces ninguna
     * escritura con acks=all puede cumplirse (NotEnoughReplicas): se rechaza aqui.
     * Un valor no numerico lo rechaza el broker con su propio mensaje.
     */
    private static void validarMinInsync(String valor, int replicas) {
        if (valor != null && valor.matches("\\d{1,9}") && Integer.parseInt(valor) > replicas) {
            throw new IllegalArgumentException("min.insync.replicas (" + valor + ") no puede superar las replicas ("
                    + replicas + "): ninguna escritura con acks=all podria completarse");
        }
    }

    private int replicasDe(String nombre) {
        TopicDescription t = esperarTopico(nombre, admin.describeTopics(List.of(nombre)).allTopicNames()).get(nombre);
        return t.partitions().get(0).replicas().size();
    }

    private static ConfigResource topico(String nombre) {
        return new ConfigResource(ConfigResource.Type.TOPIC, nombre);
    }

    private static List<Integer> ids(List<Node> nodos) {
        return nodos.stream().map(Node::id).toList();
    }

    /** Como {@link #esperar} pero con un 404 que nombra el topico, en vez de "This server does not host this topic-partition". */
    private static <T> T esperarTopico(String nombre, KafkaFuture<T> futuro) {
        try {
            return esperar(futuro);
        } catch (UnknownTopicOrPartitionException e) {
            throw new NoSuchElementException("No existe el topico " + nombre);
        }
    }

    /** Desenvuelve la ExecutionException: hacia arriba sale la excepcion real de Kafka. */
    private static <T> T esperar(KafkaFuture<T> futuro) {
        try {
            return futuro.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof KafkaException k) {
                throw k;
            }
            throw new KafkaException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("Interrumpido esperando al broker", e);
        }
    }
}
