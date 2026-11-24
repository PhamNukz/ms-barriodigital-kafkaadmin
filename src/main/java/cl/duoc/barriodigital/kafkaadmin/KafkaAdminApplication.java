package cl.duoc.barriodigital.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "cl.duoc.barriodigital")
public class KafkaAdminApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }
}
