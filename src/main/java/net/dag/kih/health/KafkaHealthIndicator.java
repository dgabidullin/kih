package net.dag.kih.health;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.dag.kih.configuration.kafka.KafkaInputHandlerProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import static org.springframework.boot.actuate.health.Status.DOWN;
import static org.springframework.boot.actuate.health.Status.UP;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaHealthIndicator implements HealthIndicator {

  private final KafkaListenerEndpointRegistry registry;
  private final KafkaInputHandlerProperties properties;

  @Override
  public Health health() {
    return registry.getListenerContainers()
        .stream()
        .filter(r -> r.getGroupId().equalsIgnoreCase(properties.getPsaTaskConsumerProps().getGroupId()))
        .findFirst()
        .map(listenerContainer -> new Health.Builder()
            .status(listenerContainer.getAssignedPartitions().size() > 0 ? UP : DOWN)
            .withDetail("assigned partition for listener", listenerContainer.getAssignedPartitions().toString())
            .build())
        .orElse(Health.down().build());
  }
}