package net.dag.kih.configuration.spark;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import scala.collection.JavaConverters;

import java.util.Map;

@Data
@Slf4j
@Configuration
@ConfigurationProperties(prefix = "spark")
public class SparkProperties {

  protected Map<String, String> commonSparkProps;

  @Bean
  public scala.collection.Map<String, String> scalaMapFromJavaProps() {
    return JavaConverters
        .mapAsScalaMapConverter(commonSparkProps)
        .asScala();
  }
}
