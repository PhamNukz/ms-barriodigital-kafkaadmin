package cl.duoc.barriodigital.kafkaadmin.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.List;
import java.util.Map;

/**
 * Los topicos del sistema son codigo: se declaran aqui como {@link NewTopic} y
 * {@link KafkaAdmin} los aplica al arrancar. Crea los que faltan y, con
 * {@code spring.kafka.admin.modify-topic-configs: true}, alinea las configs de los
 * que ya existen (p. ej. {@code requests.events}, que hoy existe porque el broker
 * lo auto-creo con sus defaults). Los valores salen de
 * {@code barriodigital.kafka.topics.*} en {@code application.yml}: agregar un
 * topico es agregar un bloque ahi, no tocar Java.
 */
@Configuration
@EnableConfigurationProperties(TopicsConfig.TopicosProperties.class)
public class TopicsConfig {

    /** Un bloque por topico; la clave (p. ej. {@code requests-events}) es solo un id logico. */
    @ConfigurationProperties(prefix = "barriodigital.kafka")
    public record TopicosProperties(Map<String, Topico> topics) {

        public TopicosProperties {
            topics = topics == null ? Map.of() : Map.copyOf(topics);
        }

        public record Topico(String nombre, int particiones, int replicas,
                             long retentionMs, String cleanupPolicy, int minInsyncReplicas) {
        }
    }

    @Bean
    KafkaAdmin.NewTopics topicosDelSistema(TopicosProperties props) {
        return new KafkaAdmin.NewTopics(nuevosTopicos(props).toArray(NewTopic[]::new));
    }

    /** Publico para poder probar el yml → NewTopic: {@code NewTopics} no expone su contenido. */
    public static List<NewTopic> nuevosTopicos(TopicosProperties props) {
        return props.topics().values().stream()
                .map(t -> TopicBuilder.name(t.nombre())
                        .partitions(t.particiones())
                        .replicas(t.replicas())
                        .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(t.retentionMs()))
                        .config(TopicConfig.CLEANUP_POLICY_CONFIG, t.cleanupPolicy())
                        .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(t.minInsyncReplicas()))
                        .build())
                .toList();
    }

    /** Mismo bootstrap y timeouts que {@code spring.kafka.admin.*}; lo usa solo {@code KafkaAdminService}. */
    @Bean(destroyMethod = "close")
    AdminClient adminClient(KafkaAdmin kafkaAdmin) {
        return AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }
}
