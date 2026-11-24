package cl.duoc.barriodigital.kafkaadmin;

import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService.LagParticion;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

/** EP5 ind. 3, 7 y 9: el servicio se prueba con AdminClient mockeado, sin broker. */
class KafkaAdminServiceTest {

    static final Node B1 = new Node(1, "kafka1", 9092);
    static final Node B2 = new Node(2, "kafka2", 9093);
    static final Node B3 = new Node(3, "kafka3", 9094);

    AdminClient admin;
    KafkaAdminService service;

    @BeforeEach
    void setUp() {
        admin = mock(AdminClient.class);
        service = new KafkaAdminService(admin);
    }

    static <T> KafkaFuture<T> ok(T valor) {
        return KafkaFuture.completedFuture(valor);
    }

    static <T> KafkaFuture<T> falla(Throwable t) {
        KafkaFutureImpl<T> f = new KafkaFutureImpl<>();
        f.completeExceptionally(t);
        return f;
    }

    static TopicDescription topico(String nombre, int particiones) {
        List<TopicPartitionInfo> ps = new ArrayList<>();
        for (int i = 0; i < particiones; i++) {
            ps.add(new TopicPartitionInfo(i, B1, List.of(B1, B2, B3), List.of(B1, B2)));
        }
        return new TopicDescription(nombre, false, ps);
    }

    void describeTopicsDevuelve(TopicDescription... ts) {
        Map<String, TopicDescription> m = new HashMap<>();
        for (TopicDescription t : ts) m.put(t.name(), t);
        DescribeTopicsResult r = mock(DescribeTopicsResult.class);
        when(r.allTopicNames()).thenReturn(ok(m));
        when(admin.describeTopics(anyCollection())).thenReturn(r);
    }

    // ---------- crear ----------

    @Test
    @SuppressWarnings("unchecked")
    void crearTopico_envia_particiones_rf_y_configs() {
        CreateTopicsResult r = mock(CreateTopicsResult.class);
        when(r.all()).thenReturn(ok(null));
        when(admin.createTopics(anyCollection())).thenReturn(r);

        service.crearTopico("pagos.events", 3, 3,
                Map.of("retention.ms", "604800000", "cleanup.policy", "delete", "min.insync.replicas", "2"));

        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(captor.capture());
        NewTopic t = captor.getValue().iterator().next();
        assertThat(t.name()).isEqualTo("pagos.events");
        assertThat(t.numPartitions()).isEqualTo(3);
        assertThat(t.replicationFactor()).isEqualTo((short) 3);
        assertThat(t.configs())
                .containsEntry("retention.ms", "604800000")
                .containsEntry("min.insync.replicas", "2");
    }

    @Test
    void crearTopico_con_config_fuera_de_la_lista_blanca_no_llega_al_broker() {
        assertThatThrownBy(() -> service.crearTopico("x", 3, 3, Map.of("segment.bytes", "1024")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("segment.bytes")
                .hasMessageContaining("retention.ms");
        verifyNoInteractions(admin);
    }

    @Test
    void crearTopico_con_min_insync_mayor_que_replicas_es_rechazado() {
        assertThatThrownBy(() -> service.crearTopico("x", 3, 2, Map.of("min.insync.replicas", "3")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("acks=all");
        verifyNoInteractions(admin);
    }

    @Test
    void crearTopico_existente_propaga_TopicExistsException_desenvuelta() {
        CreateTopicsResult r = mock(CreateTopicsResult.class);
        when(r.all()).thenReturn(falla(new TopicExistsException("Topic 'requests.events' already exists.")));
        when(admin.createTopics(anyCollection())).thenReturn(r);

        assertThatThrownBy(() -> service.crearTopico("requests.events", 3, 3, null))
                .isInstanceOf(TopicExistsException.class);
    }

    // ---------- modificar ----------

    @Test
    @SuppressWarnings("unchecked")
    void modificarConfig_usa_SET_y_DELETE_cuando_el_valor_es_null() {
        AlterConfigsResult r = mock(AlterConfigsResult.class);
        when(r.all()).thenReturn(ok(null));
        when(admin.incrementalAlterConfigs(anyMap())).thenReturn(r);
        Map<String, String> cambios = new HashMap<>();
        cambios.put("retention.ms", "86400000");
        cambios.put("cleanup.policy", null);

        service.modificarConfig("requests.events", cambios);

        ArgumentCaptor<Map<ConfigResource, Collection<AlterConfigOp>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin).incrementalAlterConfigs(captor.capture());
        Collection<AlterConfigOp> ops =
                captor.getValue().get(new ConfigResource(ConfigResource.Type.TOPIC, "requests.events"));
        assertThat(ops).extracting(op -> op.configEntry().name() + "=" + op.opType())
                .containsExactlyInAnyOrder("retention.ms=SET", "cleanup.policy=DELETE");
    }

    @Test
    void modificarConfig_min_insync_mayor_que_el_rf_actual_es_rechazado() {
        describeTopicsDevuelve(topico("requests.events", 3));

        assertThatThrownBy(() -> service.modificarConfig("requests.events", Map.of("min.insync.replicas", "4")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("replicas (3)");
        verify(admin, never()).incrementalAlterConfigs(anyMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    void aumentarParticiones_pide_el_total_nuevo() {
        CreatePartitionsResult r = mock(CreatePartitionsResult.class);
        when(r.all()).thenReturn(ok(null));
        when(admin.createPartitions(anyMap())).thenReturn(r);

        service.aumentarParticiones("requests.events", 6);

        ArgumentCaptor<Map<String, NewPartitions>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin).createPartitions(captor.capture());
        assertThat(captor.getValue().get("requests.events").totalCount()).isEqualTo(6);
    }

    @Test
    void eliminarTopico_inexistente_es_404_con_el_nombre() {
        DeleteTopicsResult r = mock(DeleteTopicsResult.class);
        when(r.all()).thenReturn(falla(new UnknownTopicOrPartitionException("This server does not host this topic-partition.")));
        when(admin.deleteTopics(anyCollection())).thenReturn(r);

        assertThatThrownBy(() -> service.eliminarTopico("fantasma"))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessageContaining("fantasma");
    }

    // ---------- listar ----------

    @Test
    void listarTopicos_informa_particiones_y_rf_ordenados_por_nombre() {
        ListTopicsResult lt = mock(ListTopicsResult.class);
        when(lt.names()).thenReturn(ok(Set.of("requests.events", "audit.timeline")));
        when(admin.listTopics()).thenReturn(lt);
        describeTopicsDevuelve(topico("requests.events", 3), topico("audit.timeline", 3));

        assertThat(service.listarTopicos()).containsExactly(
                new KafkaAdminService.TopicoResumen("audit.timeline", 3, 3),
                new KafkaAdminService.TopicoResumen("requests.events", 3, 3));
    }

    @Test
    void broker_caido_sale_como_la_excepcion_de_kafka() {
        ListTopicsResult lt = mock(ListTopicsResult.class);
        when(lt.names()).thenReturn(falla(new TimeoutException("Timed out waiting for a node assignment.")));
        when(admin.listTopics()).thenReturn(lt);

        assertThatThrownBy(() -> service.listarTopicos()).isInstanceOf(TimeoutException.class);
    }

    // ---------- consumer groups y lag ----------

    @Test
    void detalleGrupo_calcula_el_lag_como_end_offset_menos_offset_comprometido() {
        TopicPartition p0 = new TopicPartition("requests.events", 0);
        TopicPartition p1 = new TopicPartition("requests.events", 1);
        TopicPartition p2 = new TopicPartition("requests.events", 2);

        MemberDescription m1 = new MemberDescription("m1", "consumer-report-svc-1", "/10.0.1.29",
                new MemberAssignment(Set.of(p0, p2)));
        MemberDescription m2 = new MemberDescription("m2", "consumer-report-svc-2", "/10.0.1.29",
                new MemberAssignment(Set.of(p1)));
        DescribeConsumerGroupsResult dg = mock(DescribeConsumerGroupsResult.class);
        when(dg.all()).thenReturn(ok(Map.of("report-svc", new ConsumerGroupDescription("report-svc", false,
                List.of(m1, m2), "range", ConsumerGroupState.STABLE, B1))));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(dg);

        // p2 asignada pero sin commit todavia
        ListConsumerGroupOffsetsResult lo = mock(ListConsumerGroupOffsetsResult.class);
        when(lo.partitionsToOffsetAndMetadata()).thenReturn(ok(Map.of(
                p0, new OffsetAndMetadata(10), p1, new OffsetAndMetadata(5))));
        when(admin.listConsumerGroupOffsets("report-svc")).thenReturn(lo);

        ListOffsetsResult fin = mock(ListOffsetsResult.class);
        when(fin.all()).thenReturn(ok(Map.of(
                p0, new ListOffsetsResult.ListOffsetsResultInfo(12, -1, Optional.empty()),
                p1, new ListOffsetsResult.ListOffsetsResultInfo(5, -1, Optional.empty()),
                p2, new ListOffsetsResult.ListOffsetsResultInfo(3, -1, Optional.empty()))));
        when(admin.listOffsets(anyMap())).thenReturn(fin);

        var g = service.detalleGrupo("report-svc");

        assertThat(g.estado()).isEqualTo("STABLE");
        assertThat(g.miembros()).isEqualTo(2);
        assertThat(g.lagTotal()).isEqualTo(2);
        assertThat(g.particiones()).containsExactly(
                new LagParticion("requests.events", 0, "consumer-report-svc-1", 10L, 12, 2L),
                new LagParticion("requests.events", 1, "consumer-report-svc-2", 5L, 5, 0L),
                new LagParticion("requests.events", 2, "consumer-report-svc-1", null, 3, null));
    }

    @Test
    void detalleGrupo_inexistente_kafka_lo_describe_DEAD_y_es_404() {
        DescribeConsumerGroupsResult dg = mock(DescribeConsumerGroupsResult.class);
        when(dg.all()).thenReturn(ok(Map.of("nadie", new ConsumerGroupDescription("nadie", false,
                List.of(), "", ConsumerGroupState.DEAD, B1))));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(dg);

        assertThatThrownBy(() -> service.detalleGrupo("nadie"))
                .isInstanceOf(NoSuchElementException.class)
                .hasMessageContaining("nadie");
        verify(admin, never()).listConsumerGroupOffsets(org.mockito.ArgumentMatchers.anyString());
    }
}
