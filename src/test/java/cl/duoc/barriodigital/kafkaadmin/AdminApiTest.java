package cl.duoc.barriodigital.kafkaadmin;

import cl.duoc.barriodigital.kafkaadmin.config.TopicsConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static cl.duoc.barriodigital.kafkaadmin.KafkaAdminServiceTest.falla;
import static cl.duoc.barriodigital.kafkaadmin.KafkaAdminServiceTest.ok;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * EP5 instruccion 12 (crear, modificar y eliminar topicos y configs) e ind. 3/9:
 * la API existe, esta gateada por rol y valida con mensajes legibles. El broker
 * se reemplaza por un AdminClient mockeado: no hace falta Kafka corriendo.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class AdminApiTest {

    @Autowired
    MockMvc mvc;

    @MockBean
    JwtDecoder jwtDecoder;

    @MockBean
    AdminClient admin;

    private static SimpleGrantedAuthority rol(String r) {
        return new SimpleGrantedAuthority("ROLE_" + r);
    }

    private static final String TOPICO_OK =
            "{\"nombre\":\"pagos.events\",\"particiones\":3,\"replicas\":3,"
            + "\"configs\":{\"retention.ms\":\"604800000\",\"min.insync.replicas\":\"2\"}}";

    // ---------- seguridad ----------

    @Test
    void sin_token_401() throws Exception {
        mvc.perform(get("/admin/topics")).andExpect(status().isUnauthorized());
    }

    @Test
    void con_rol_distinto_de_admin_403() throws Exception {
        mvc.perform(get("/admin/consumer-groups").with(jwt().authorities(rol("Funcionario"))))
           .andExpect(status().isForbidden());
    }

    @Test
    void crear_topico_sin_rol_admin_403_y_no_toca_el_broker() throws Exception {
        mvc.perform(post("/admin/topics").contentType("application/json").content(TOPICO_OK)
                .with(jwt().authorities(rol("Vecino"))))
           .andExpect(status().isForbidden());
        verifyNoInteractions(admin);
    }

    @Test
    void actuator_health_queda_abierto_sin_token() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk());
    }

    // ---------- camino feliz ----------

    @Test
    void crear_topico_con_rol_admin_201() throws Exception {
        CreateTopicsResult r = mock(CreateTopicsResult.class);
        when(r.all()).thenReturn(ok(null));
        when(admin.createTopics(anyCollection())).thenReturn(r);

        mvc.perform(post("/admin/topics").contentType("application/json").content(TOPICO_OK)
                .with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isCreated())
           .andExpect(jsonPath("$.nombre").value("pagos.events"))
           .andExpect(jsonPath("$.configs['retention.ms']").value("604800000"));
    }

    @Test
    void modificar_config_con_rol_admin_204() throws Exception {
        AlterConfigsResult r = mock(AlterConfigsResult.class);
        when(r.all()).thenReturn(ok(null));
        when(admin.incrementalAlterConfigs(org.mockito.ArgumentMatchers.anyMap())).thenReturn(r);

        mvc.perform(patch("/admin/topics/requests.events/config").contentType("application/json")
                .content("{\"configs\":{\"retention.ms\":\"86400000\"}}")
                .with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isNoContent());
    }

    @Test
    void cluster_lista_brokers_y_controller() throws Exception {
        DescribeClusterResult r = mock(DescribeClusterResult.class);
        Node b1 = new Node(1, "kafka1", 9092);
        when(r.clusterId()).thenReturn(ok("abc"));
        when(r.controller()).thenReturn(ok(b1));
        when(r.nodes()).thenReturn(ok(List.of(new Node(2, "kafka2", 9093), b1)));
        when(admin.describeCluster()).thenReturn(r);

        mvc.perform(get("/admin/cluster").with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isOk())
           .andExpect(jsonPath("$.controller").value(1))
           .andExpect(jsonPath("$.brokers[0].id").value(1))
           .andExpect(jsonPath("$.brokers.length()").value(2));
    }

    // ---------- validacion, con mensaje legible ----------

    @Test
    void nombre_con_espacios_es_400_y_explica_que_caracteres_valen() throws Exception {
        mvc.perform(post("/admin/topics").contentType("application/json")
                .content("{\"nombre\":\"mi topico\",\"particiones\":3,\"replicas\":3}")
                .with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.title").value("Entrada invalida"))
           .andExpect(jsonPath("$.detail").value(containsString("sin espacios")));
    }

    @Test
    void replicas_4_es_400_porque_hay_3_brokers() throws Exception {
        mvc.perform(post("/admin/topics").contentType("application/json")
                .content("{\"nombre\":\"x\",\"particiones\":3,\"replicas\":4}")
                .with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.detail").value(containsString("entre 1 y 3")));
    }

    @Test
    void particiones_0_es_400() throws Exception {
        mvc.perform(post("/admin/topics").contentType("application/json")
                .content("{\"nombre\":\"x\",\"particiones\":0,\"replicas\":3}")
                .with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.detail").value(containsString("al menos 1")));
    }

    @Test
    void config_fuera_de_la_lista_blanca_es_400_y_lista_las_permitidas() throws Exception {
        mvc.perform(patch("/admin/topics/requests.events/config").contentType("application/json")
                .content("{\"configs\":{\"segment.bytes\":\"1024\"}}")
                .with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.detail").value(containsString("segment.bytes")))
           .andExpect(jsonPath("$.detail").value(containsString("cleanup.policy")));
    }

    @Test
    void patch_sin_configs_es_400() throws Exception {
        mvc.perform(patch("/admin/topics/requests.events/config").contentType("application/json")
                .content("{\"configs\":{}}")
                .with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.detail").value(containsString("al menos una clave")));
    }

    @Test
    void borrar_topico_interno_es_400_sin_llegar_al_broker() throws Exception {
        mvc.perform(delete("/admin/topics/__consumer_offsets").with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isBadRequest())
           .andExpect(jsonPath("$.detail").value(containsString("'__'")));
        verifyNoInteractions(admin);
    }

    // ---------- 404, 409, 502 ----------

    @Test
    void eliminar_topico_inexistente_es_404_con_el_nombre() throws Exception {
        DeleteTopicsResult r = mock(DeleteTopicsResult.class);
        when(r.all()).thenReturn(falla(new UnknownTopicOrPartitionException("This server does not host this topic-partition.")));
        when(admin.deleteTopics(anyCollection())).thenReturn(r);

        mvc.perform(delete("/admin/topics/fantasma").with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isNotFound())
           .andExpect(jsonPath("$.detail").value(containsString("fantasma")));
    }

    @Test
    void consumer_group_inexistente_es_404() throws Exception {
        DescribeConsumerGroupsResult r = mock(DescribeConsumerGroupsResult.class);
        when(r.all()).thenReturn(ok(Map.of("nadie", new ConsumerGroupDescription("nadie", false,
                List.of(), "", ConsumerGroupState.DEAD, new Node(1, "kafka1", 9092)))));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(r);

        mvc.perform(get("/admin/consumer-groups/nadie").with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isNotFound())
           .andExpect(jsonPath("$.detail").value(containsString("nadie")));
    }

    @Test
    void crear_topico_que_ya_existe_es_409() throws Exception {
        CreateTopicsResult r = mock(CreateTopicsResult.class);
        when(r.all()).thenReturn(falla(new TopicExistsException("Topic 'pagos.events' already exists.")));
        when(admin.createTopics(anyCollection())).thenReturn(r);

        mvc.perform(post("/admin/topics").contentType("application/json").content(TOPICO_OK)
                .with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isConflict())
           .andExpect(jsonPath("$.detail").value(containsString("already exists")));
    }

    @Test
    void broker_caido_es_502() throws Exception {
        ListTopicsResult r = mock(ListTopicsResult.class);
        when(r.names()).thenReturn(falla(new TimeoutException("Timed out waiting for a node assignment.")));
        when(admin.listTopics()).thenReturn(r);

        mvc.perform(get("/admin/topics").with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isBadGateway())
           .andExpect(jsonPath("$.title").value("Error del broker"));
    }

    // ---------- topicos declarados (ind. 7) ----------

    @Autowired
    TopicsConfig.TopicosProperties topicos;

    /** Lo que KafkaAdmin aplicara al arrancar sale del application.yml, con sus configs. */
    @Test
    void los_4_topicos_del_yml_se_declaran_con_particiones_rf_y_configs() {
        Map<String, NewTopic> porNombre = new java.util.HashMap<>();
        TopicsConfig.nuevosTopicos(topicos).forEach(t -> porNombre.put(t.name(), t));

        org.assertj.core.api.Assertions.assertThat(porNombre).containsOnlyKeys(
                "requests.events", "audit.timeline", "requests.events.DLT", "audit.timeline.DLT");
        porNombre.values().forEach(t -> {
            org.assertj.core.api.Assertions.assertThat(t.numPartitions()).isEqualTo(3);
            org.assertj.core.api.Assertions.assertThat(t.replicationFactor()).isEqualTo((short) 3);
            org.assertj.core.api.Assertions.assertThat(t.configs()).containsEntry("min.insync.replicas", "2");
        });
        org.assertj.core.api.Assertions.assertThat(porNombre.get("audit.timeline").configs())
                .containsEntry("cleanup.policy", "compact,delete")
                .containsEntry("retention.ms", "2592000000");
        org.assertj.core.api.Assertions.assertThat(porNombre.get("requests.events.DLT").configs())
                .containsEntry("retention.ms", "1209600000");
    }

    @Test
    void listar_topicos_con_rol_admin_200() throws Exception {
        ListTopicsResult lt = mock(ListTopicsResult.class);
        when(lt.names()).thenReturn(ok(Set.of()));
        when(admin.listTopics()).thenReturn(lt);

        mvc.perform(get("/admin/topics").with(jwt().authorities(rol("Admin"))))
           .andExpect(status().isOk())
           .andExpect(jsonPath("$").isArray());
    }
}
