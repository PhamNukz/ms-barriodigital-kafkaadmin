package cl.duoc.barriodigital.kafkaadmin.web;

import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService.GrupoDetalle;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService.GrupoResumen;
import jakarta.validation.constraints.Pattern;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static cl.duoc.barriodigital.kafkaadmin.web.AdminDtos.ID_GRUPO;
import static cl.duoc.barriodigital.kafkaadmin.web.AdminDtos.MENSAJE_GRUPO;

/** Solo lectura: inspeccionar grupos y lag (equivale a kafka-consumer-groups.sh --describe). */
@Validated
@RestController
@RequestMapping("/admin/consumer-groups")
public class ConsumerGroupController {

    private final KafkaAdminService service;

    public ConsumerGroupController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping
    public List<GrupoResumen> listar() {
        return service.listarGrupos();
    }

    @GetMapping("/{id}")
    public GrupoDetalle detalle(@PathVariable @Pattern(regexp = ID_GRUPO, message = MENSAJE_GRUPO) String id) {
        return service.detalleGrupo(id);
    }
}
