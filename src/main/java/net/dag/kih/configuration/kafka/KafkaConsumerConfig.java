package net.dag.kih.configuration.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.dag.kih.model.Task;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.LogIfLevelEnabled;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@Slf4j
@Configuration
@EnableKafka
@RequiredArgsConstructor
public class KafkaConsumerConfig implements KafkaListenerConfigurer {

  private final KafkaInputHandlerProperties properties;
  private final MeterRegistry meterRegistry;

  @Bean
  public ConsumerFactory<String, Task> getInstancePsaTaskObjectFactory() {
    Map<String, Object> props = properties.getPsaTaskConsumerProps().getProps();
    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Override
  public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
    registrar.setMessageHandlerMethodFactory(kafkaHandlerMethodFactory());
  }

  @Bean
  public DefaultMessageHandlerMethodFactory kafkaHandlerMethodFactory() {
    DefaultMessageHandlerMethodFactory factory = new DefaultMessageHandlerMethodFactory();
    return factory;
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, Task> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(getInstancePsaTaskObjectFactory());
    factory.setConcurrency(properties.getPsaTaskConsumerProps().partitionNum);
    factory.setErrorHandler(new ErrorHandler() {
      @Override
      public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {

      }

      @Override
      public void handle(Exception e, ConsumerRecord<?, ?> consumerRecord) {

      }

      @Override
      public void handle(Exception e, ConsumerRecord<?, ?> consumerRecord, Consumer<?, ?> consumer) {
        log.error("Exception occurs {}", e.getMessage());
        log.error("Income bad model for PsaTask - {}", consumerRecord.value());

        String topics = consumerRecord.topic();
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();

        TopicPartition topicPartition = new TopicPartition(topics, partition);
        log.warn("Skipping {}-{} offset {}", topics, partition, offset);

        consumer.commitSync();
        consumer.seek(topicPartition, offset + 1);

        Timer timer = meterRegistry.timer("psaTask.processing",
            "exception", e.getClass().getSimpleName(),
            "class", "org.springframework.kafka.listener.ErrorHandler",
            "method", "handle");
        timer.record(1L, TimeUnit.MILLISECONDS);
      }
    });
    factory.getContainerProperties().setPollTimeout(1000L);
    factory.setMessageConverter(new StringJsonMessageConverter());
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    factory.getContainerProperties().setCommitLogLevel(LogIfLevelEnabled.Level.INFO);
    return factory;
  }
}
