package net.dag.kih.health;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SparkHealthIndicator implements HealthIndicator {

  private final SparkSession sparkSession;

  @Override
  public Health health() {
    return this.sparkSession.sparkContext().isStopped() ? Health.down().build() : Health.up().build();
  }
}
