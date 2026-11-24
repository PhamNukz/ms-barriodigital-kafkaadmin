package cl.duoc.barriodigital.kafkaadmin.web;

import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService.Cluster;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClusterController {

    private final KafkaAdminService service;

    public ClusterController(KafkaAdminService service) {
        this.service = service;
    }

    /** Id del cluster, controller y los brokers vivos. */
    @GetMapping("/admin/cluster")
    public Cluster cluster() {
        return service.cluster();
    }
}
