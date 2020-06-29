package net.dag.kih.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import net.dag.kih.model.Task;
import net.dag.kih.model.TaskResult;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

import static net.dag.kih.model.TaskResult.Status.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(partitions = 1, topics = {
    "load.tvi.paneldemrespondentvalue",
    "${kafka.psa-task-consumer-props.topic-name}",
    "${kafka.psa-task-result-producer-props.topic-name}",
})
@ActiveProfiles(profiles = {"test"})
public class TaskServiceImplTest {

  @Value("${kafka.psa-task-consumer-props.topic-name}")
  private String psaTaskTopicName;
  @Value("${kafka.psa-task-result-producer-props.topic-name}")
  private String psaTaskResultTopicName;
  @Value("${java.io.tmpdir}")
  private String tmpDir;
  @Autowired
  private KafkaTemplate<String, Task> psaTaskKafkaTemplate;
  @Autowired
  private KafkaTemplate<String, byte[]> avroKafkaTemplate;
  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private Consumer<String, TaskResult> consumer;
  @Autowired
  private SparkSession sparkSession;

  @PostConstruct
  public void postConstruct() {
    consumer.subscribe(Collections.singletonList(psaTaskResultTopicName));
  }

  @Before
  public void before() throws IOException {
    FileUtils.deleteDirectory(new File(tmpDir + "/version=123"));
    FileUtils.deleteDirectory(new File(tmpDir + "/latest"));
  }

  @Test
  public void testTaskWithoutKeys() {
    sendAvroFileToKafka("load.tvi.paneldemrespondentvalue", "data/avro_record2.avro");
    String path = tmpDir + "/version=123";
    sendPsaTaskToKafka("data/task_without_keys.json", path);
    ConsumerRecords<String, TaskResult> records = consumer.poll(Duration.ofSeconds(10));
    assertEquals(1, records.count());
    records.forEach(record -> assertEquals(OK, record.value().getStatus()));
    assertParquetEquals("src/test/resources/data/data.snappy.parquet", path);
    assertParquetEquals("src/test/resources/data/data.snappy.parquet", tmpDir + "/latest");
  }

  @Test
  public void testTaskWithKeys() {
    sendAvroFileToKafka("load.tvi.paneldemrespondentvalue", "data/avro_record2.avro");
    String path = tmpDir + "/version=123";
    sendPsaTaskToKafka("data/task_with_keys.json", path);
    ConsumerRecords<String, TaskResult> records = consumer.poll(Duration.ofSeconds(5));
    assertEquals(1, records.count());
    records.forEach(record -> assertEquals(OK, record.value().getStatus()));
    assertParquetEquals("src/test/resources/data/data.snappy.parquet", path);
    assertFalse(new File(tmpDir + "latest").exists());
  }

  @SneakyThrows
  private void sendPsaTaskToKafka(String taskFile, String parquetPath) {
    Task task = objectMapper.readValue(readFromResources(taskFile), Task.class);
    task.getParams().setPath(parquetPath);
    psaTaskKafkaTemplate.send(psaTaskTopicName, task);
  }

  private void sendAvroFileToKafka(String topic, String avroFile) {
    byte[] avro_record = readFromResources(avroFile);
    avroKafkaTemplate.send(topic, avro_record);
  }

  @SneakyThrows
  private byte[] readFromResources(String fileName) {
    return FileUtils.readFileToByteArray(new ClassPathResource(fileName).getFile());
  }

  private void assertParquetEquals(String path1, String path2) {
    Dataset<Row> dataset1 = sparkSession.sqlContext().parquetFile(path1);
    Dataset<Row> dataset2 = sparkSession.sqlContext().parquetFile(path2);
    // symmetric difference
    assertEquals(0, dataset1.unionByName(dataset2).except(dataset1.intersect(dataset2)).count());
  }
}
