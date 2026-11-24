package cl.duoc.barriodigital.kafkaadmin.web;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * Traduce los fallos a ProblemDetail con un mensaje que se pueda leer sin abrir
 * el codigo: que campo fallo y por que. Spring elige el handler de la excepcion
 * mas especifica, asi que {@link KafkaException} (502) solo atrapa lo que no
 * calza en los de arriba.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    /** Cuerpo invalido (@Valid @RequestBody). */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail entradaInvalida(MethodArgumentNotValidException ex) {
        String detalle = ex.getBindingResult().getFieldErrors().stream()
                .map(e -> e.getField() + ": " + e.getDefaultMessage())
                .sorted()
                .collect(Collectors.joining("; "));
        return problema(HttpStatus.BAD_REQUEST, "Entrada invalida", detalle);
    }

    /** Nombre invalido en la ruta (controladores con @Validated). */
    @ExceptionHandler(ConstraintViolationException.class)
    ProblemDetail parametroInvalido(ConstraintViolationException ex) {
        String detalle = ex.getConstraintViolations().stream()
                .map(ConstraintViolation::getMessage)
                .sorted()
                .collect(Collectors.joining("; "));
        return problema(HttpStatus.BAD_REQUEST, "Entrada invalida", detalle);
    }

    /** Config fuera de la lista blanca, min.insync > replicas. */
    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail argumentoInvalido(IllegalArgumentException ex) {
        return problema(HttpStatus.BAD_REQUEST, "Entrada invalida", ex.getMessage());
    }

    /** Topico o consumer group inexistente. */
    @ExceptionHandler({NoSuchElementException.class, UnknownTopicOrPartitionException.class,
            GroupIdNotFoundException.class})
    ProblemDetail noEncontrado(Exception ex) {
        return problema(HttpStatus.NOT_FOUND, "No encontrado", ex.getMessage());
    }

    @ExceptionHandler(TopicExistsException.class)
    ProblemDetail yaExiste(TopicExistsException ex) {
        return problema(HttpStatus.CONFLICT, "El topico ya existe", ex.getMessage());
    }

    /**
     * Kafka valido la peticion y la rechazo: valor de config invalido
     * ({@code cleanup.policy=borrar}), reducir particiones, RF mayor que los brokers
     * vivos. Es un error de entrada: 400 con el motivo del broker.
     */
    @ExceptionHandler({InvalidTopicException.class, InvalidPartitionsException.class,
            InvalidReplicationFactorException.class, InvalidConfigurationException.class,
            InvalidRequestException.class, PolicyViolationException.class})
    ProblemDetail rechazadoPorKafka(ApiException ex) {
        return problema(HttpStatus.BAD_REQUEST, "Kafka rechazo la operacion", ex.getMessage());
    }

    /** Broker caido, timeout o cualquier otra falla de abajo: el error no es de este servicio. */
    @ExceptionHandler(KafkaException.class)
    ProblemDetail brokerNoDisponible(KafkaException ex) {
        return problema(HttpStatus.BAD_GATEWAY, "Error del broker",
                "Kafka no respondio o fallo: " + ex.getMessage());
    }

    private static ProblemDetail problema(HttpStatus status, String titulo, String detalle) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detalle);
        pd.setTitle(titulo);
        return pd;
    }
}
