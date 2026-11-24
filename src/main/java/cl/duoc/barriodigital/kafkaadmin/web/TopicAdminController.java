package cl.duoc.barriodigital.kafkaadmin.web;

import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService.TopicoDetalle;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService.TopicoResumen;
import cl.duoc.barriodigital.kafkaadmin.web.AdminDtos.AumentarParticionesRequest;
import cl.duoc.barriodigital.kafkaadmin.web.AdminDtos.CrearTopicoRequest;
import cl.duoc.barriodigital.kafkaadmin.web.AdminDtos.ModificarConfigRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static cl.duoc.barriodigital.kafkaadmin.web.AdminDtos.MENSAJE_TOPICO;
import static cl.duoc.barriodigital.kafkaadmin.web.AdminDtos.NOMBRE_TOPICO;

/**
 * Acceso restringido a Admin en SecurityConfig, para todo /admin/**.
 * {@code @Validated} hace que el nombre de la ruta se valide igual que el del cuerpo.
 */
@Validated
@RestController
@RequestMapping("/admin/topics")
public class TopicAdminController {

    private final KafkaAdminService service;

    public TopicAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping
    public List<TopicoResumen> listar() {
        return service.listarTopicos();
    }

    @GetMapping("/{nombre}")
    public TopicoDetalle detalle(@PathVariable @Pattern(regexp = NOMBRE_TOPICO,
            message = "el nombre del topico " + MENSAJE_TOPICO) String nombre) {
        return service.detalleTopico(nombre);
    }

    /** Devuelve lo que se creo. */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CrearTopicoRequest crear(@Valid @RequestBody CrearTopicoRequest req) {
        service.crearTopico(req.nombre(), req.particiones(), req.replicas(), req.configs());
        return req;
    }

    @PatchMapping("/{nombre}/config")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void modificarConfig(@PathVariable @Pattern(regexp = NOMBRE_TOPICO,
                                        message = "el nombre del topico " + MENSAJE_TOPICO) String nombre,
                                @Valid @RequestBody ModificarConfigRequest req) {
        service.modificarConfig(nombre, req.configs());
    }

    @PostMapping("/{nombre}/partitions")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void aumentarParticiones(@PathVariable @Pattern(regexp = NOMBRE_TOPICO,
                                            message = "el nombre del topico " + MENSAJE_TOPICO) String nombre,
                                    @Valid @RequestBody AumentarParticionesRequest req) {
        service.aumentarParticiones(nombre, req.particiones());
    }

    @DeleteMapping("/{nombre}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void eliminar(@PathVariable @Pattern(regexp = NOMBRE_TOPICO,
            message = "el nombre del topico " + MENSAJE_TOPICO) String nombre) {
        service.eliminarTopico(nombre);
    }
}
