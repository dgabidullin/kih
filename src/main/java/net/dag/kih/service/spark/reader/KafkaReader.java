package net.dag.kih.service.spark.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import lombok.extern.slf4j.Slf4j;
import net.dag.kih.configuration.kafka.KafkaInputHandlerProperties;
import net.dag.kih.model.Task;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;


@Slf4j
@Service
public class KafkaReader<T extends Task> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final KafkaInputHandlerProperties kafkaInputHandlerProperties;

  private final ObjectMapper mapper;

  @Autowired
  public KafkaReader(KafkaInputHandlerProperties kafkaInputHandlerProperties, ObjectMapper mapper) {
    this.mapper = mapper;
    this.kafkaInputHandlerProperties = kafkaInputHandlerProperties;
  }

  public Dataset<Row> loadRangeOffsets(SparkSession sparkSession, Map<String, String> options, T psaTask) throws JsonProcessingException {
    log.info("[{}] Incoming options {}", psaTask.getId(), options);
    log.info("[{}] Incoming psaTask {}", psaTask.getId(), psaTask);
    log.info("[{}] StartingOffsets {}", psaTask.getId(), mapper.writeValueAsString(psaTask.getKafkaSource().getStartingOffsets()));
    log.info("[{}] EndingOffsets {}", psaTask.getId(), mapper.writeValueAsString(psaTask.getKafkaSource().getEndingOffsets()));
    return sparkSession
        .read()
        .options(options)
        .option("kafka.security.protocol", kafkaInputHandlerProperties.getPsaTaskConsumerProps().getSecurityProtocol())
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaInputHandlerProperties.getPsaTaskConsumerProps().getBootstrapServers())
        .option("subscribe", psaTask.getKafkaSource().getTopic())
        .option("startingOffsets", mapper.writeValueAsString(psaTask.getKafkaSource().getStartingOffsets()))
        .option("endingOffsets", mapper.writeValueAsString(psaTask.getKafkaSource().getEndingOffsets()))
        .load();
  }
}
