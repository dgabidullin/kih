package net.dag.kih.configuration.kafka;

import net.dag.kih.model.TaskResult;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@Configuration
public class KafkaProducerConfig {
  @Bean
  public KafkaTemplate<String, TaskResult> templatePsaTaskResult(KafkaInputHandlerProperties properties) {
    return new KafkaTemplate<>(getInstanceProducerFactoryPsaTaskResult(properties));
  }

  @Bean
  public ProducerFactory<String, TaskResult> getInstanceProducerFactoryPsaTaskResult(KafkaInputHandlerProperties properties) {
    return new DefaultKafkaProducerFactory<>(properties.getPsaTaskResultProducerProps().getProps(), new StringSerializer(), new JsonSerializer<>());
  }
}
