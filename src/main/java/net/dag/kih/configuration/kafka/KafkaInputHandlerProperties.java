package net.dag.kih.configuration.kafka;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.io.Serializable;

@Data
@ToString
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaInputHandlerProperties implements Serializable {

  private static final long serialVersionUID = 1L;

  KafkaCommonProperties psaTaskConsumerProps;

  KafkaCommonProperties psaTaskResultProducerProps;
}
