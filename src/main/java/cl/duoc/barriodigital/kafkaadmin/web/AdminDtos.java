package cl.duoc.barriodigital.kafkaadmin.web;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.util.Map;

/**
 * Entrada de la API. El nombre se valida contra lo que Kafka acepta —letras,
 * digitos, punto, guion y guion bajo, hasta 249 caracteres— y sin el prefijo
 * {@code __}, que es de los topicos internos ({@code __consumer_offsets}): borrar
 * ese es perder los offsets de todos los grupos. Las mismas reglas se aplican al
 * nombre que llega en la ruta. La lista blanca de configs y la regla
 * min.insync ≤ replicas viven en {@code KafkaAdminService}, porque valen tanto
 * para crear como para modificar.
 */
public final class AdminDtos {

    private AdminDtos() {
    }

    static final String NOMBRE_TOPICO = "^(?!__)[A-Za-z0-9._-]{1,249}$";
    static final String MENSAJE_TOPICO =
            "solo admite letras, digitos, punto, guion y guion bajo (sin espacios), hasta 249 caracteres,"
            + " y no puede empezar con '__' (prefijo de los topicos internos de Kafka)";

    static final String ID_GRUPO = "^[A-Za-z0-9._-]{1,255}$";
    static final String MENSAJE_GRUPO =
            "el id del consumer group solo admite letras, digitos, punto, guion y guion bajo, hasta 255 caracteres";

    static final String MENSAJE_REPLICAS = "replicas debe estar entre 1 y 3: el cluster tiene 3 brokers";

    public record CrearTopicoRequest(
            @NotBlank(message = "el nombre del topico es obligatorio")
            @Pattern(regexp = NOMBRE_TOPICO, message = "el nombre del topico " + MENSAJE_TOPICO)
            String nombre,

            @NotNull(message = "particiones es obligatorio")
            @Min(value = 1, message = "particiones debe ser al menos 1")
            Integer particiones,

            @NotNull(message = "replicas es obligatorio")
            @Min(value = 1, message = MENSAJE_REPLICAS)
            @Max(value = 3, message = MENSAJE_REPLICAS)
            Integer replicas,

            /** Opcional; claves dentro de la lista blanca, p. ej. {"retention.ms": "604800000"}. */
            Map<String, String> configs) {
    }

    /** Claves a cambiar; un valor null quita el override y vuelve al default del broker. */
    public record ModificarConfigRequest(
            @NotEmpty(message = "configs es obligatorio y debe traer al menos una clave")
            Map<String, String> configs) {
    }

    /** Total nuevo, no cuantas agregar: es la semantica de Kafka (NewPartitions.increaseTo). */
    public record AumentarParticionesRequest(
            @NotNull(message = "particiones es obligatorio")
            @Min(value = 1, message = "particiones debe ser al menos 1")
            Integer particiones) {
    }
}
