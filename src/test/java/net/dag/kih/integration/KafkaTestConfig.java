package net.dag.kih.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.dag.kih.configuration.kafka.KafkaInputHandlerProperties;
import net.dag.kih.model.Task;
import net.dag.kih.model.TaskResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTestConfig {

  @Autowired
  private KafkaInputHandlerProperties properties;
  @Autowired
  private ObjectMapper objectMapper;
  @Value("${spring.embedded.kafka.brokers}")
  private String bootstrapServers;

  @Bean
  public Consumer<String, TaskResult> psaTaskResultConsumer() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "result.test.group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return new DefaultKafkaConsumerFactory<>(
        props, new StringDeserializer(),
        new JsonDeserializer<>(TaskResult.class, objectMapper)
    ).createConsumer();
  }

  @Bean
  public Map<String, Object> producerAvroConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        org.apache.kafka.common.serialization.ByteArraySerializer.class);
    return props;
  }

  @Bean
  public ProducerFactory<String, Task> psaTaskProducerFactory() {
    return new DefaultKafkaProducerFactory<>(properties.getPsaTaskConsumerProps().getProps(),
        new StringSerializer(),
        new JsonSerializer<>(objectMapper));
  }

  @Bean
  public ProducerFactory<String, byte[]> avroProducerFactory() {
    return new DefaultKafkaProducerFactory<>(producerAvroConfigs());
  }

  @Bean
  public KafkaTemplate<String, Task> psaTaskKafkaTemplate() {
    return new KafkaTemplate<>(psaTaskProducerFactory());
  }

  @Bean
  public KafkaTemplate<String, byte[]> avroKafkaTemplate() {
    return new KafkaTemplate<>(avroProducerFactory());
  }
}
