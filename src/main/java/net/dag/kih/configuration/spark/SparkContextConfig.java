package net.dag.kih.configuration.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

@SpringBootConfiguration
public class SparkContextConfig {

  private final SparkProperties properties;

  @Autowired
  public SparkContextConfig(SparkProperties properties) {
    this.properties = properties;
  }

  @Bean
  @Profile("cluster")
  public SparkConf sparkConf() {
    return new SparkConf().setAppName("kafka-input-handler");
  }

  @Bean
  @Profile("!cluster")
  public SparkConf sparkLocalConf() {
    return new SparkConf().setAppName("kafka-input-handler").setAll(properties.scalaMapFromJavaProps());
  }

  @Bean(destroyMethod = "stop")
  public SparkSession sparkSession(SparkConf sparkConf) {

    return SparkSession
        .builder()
        .appName("kafka-input-handler")
        .config(sparkConf)
        .getOrCreate();
  }
}
