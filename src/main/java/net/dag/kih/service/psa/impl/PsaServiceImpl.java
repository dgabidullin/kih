package net.dag.kih.service.psa.impl;

import lombok.extern.slf4j.Slf4j;
import net.dag.kih.service.avro.AvroConverter;
import net.dag.kih.service.psa.PsaService;
import net.dag.kih.service.spark.reader.KafkaReader;
import net.dag.kih.service.spark.writer.DatasetWriter;
import net.dag.kih.validator.DatasetValidator;
import net.dag.kih.configuration.spark.SparkProperties;
import net.dag.kih.exception.SparkDataIOException;
import net.dag.kih.model.Task;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class PsaServiceImpl implements PsaService<Row, Task> {

  private static final long serialVersionUID = 1L;

  private final SparkSession sparkSession;
  private final AvroConverter avroConverter;
  private final DatasetValidator<Row, Task> datasetValidator;
  private final DatasetWriter<Row> parquetWriter;
  private final KafkaReader<Task> kafkaReader;
  private final SparkProperties sparkProperties;

  @Autowired
  public PsaServiceImpl(
      SparkSession sparkSession,
      AvroConverter avroConverter,
      DatasetValidator<Row, Task> datasetValidator,
      DatasetWriter<Row> parquetWriter,
      KafkaReader<Task> psaTaskKafkaReader,
      SparkProperties sparkProperties
  ) {
    this.sparkSession = sparkSession;
    this.avroConverter = avroConverter;
    this.datasetValidator = datasetValidator;
    this.kafkaReader = psaTaskKafkaReader;
    this.sparkProperties = sparkProperties;
    this.parquetWriter = parquetWriter;
  }

  @Override
  public Dataset<Row> processDataset(Task task) {
    try {
      log.info("[{}] Additional spark properties: {}", task.getId(), sparkProperties);

      ExpressionEncoder<Row> encoder = RowEncoder.apply(avroConverter.convertToStructType(task.getSchema()));
      log.info("[{}] Get structure type from psaTask schema: {}", task.getId(), task.getSchema());

      if (this.datasetValidator.validateNoRecords(task)) {
        return emptyDataset(sparkSession, encoder);
      }

      log.info("[{}] Reading records from kafka topic {}", task.getId(), task.getKafkaSource().getTopic());
      Dataset<Row> ds = kafkaReader.loadRangeOffsets(sparkSession, sparkProperties.getCommonSparkProps(), task);

      //случай, когда указаны некорректные оффсеты
      if (datasetValidator.isEmptyDataset(ds)) {
        throw new IllegalArgumentException("cant find in kafka with offsets=" + task.getKafkaSource());
      }

      log.info("[{}] Validating df records and total messages records", task.getId());
      this.datasetValidator.matchRecordsFromDfWithEventRecords(ds, task);

      log.info("[{}] Decoding incoming records from kafka with avro schema", task.getId());
      Dataset<Row> recordDf = decodeWithAvroSchema(ds, this.avroConverter, encoder, task);
      log.info("[{}] Successful decoding incoming records from kafka with avro schema", task.getId());

      log.info("[{}] Validating df records and total records", task.getId());
      this.datasetValidator.matchCountAfterDecodeAvroWithEventRecords(recordDf, task);

      return recordDf;
    } catch (Exception e) {
      throw new SparkDataIOException("[" + task.getId() + "] Spark error: " + e.getMessage(), e);
    }
  }

  @Override
  public void save(Dataset<Row> df, String destPath, int numberOfPartitions, SaveMode saveMode) {
    try {
      parquetWriter.write(df, destPath, numberOfPartitions, saveMode);
    } catch (Exception e) {
      throw new SparkDataIOException("Spark error: " + e.getMessage(), e);
    }
  }


  private Dataset<Row> emptyDataset(SparkSession sparkSession, ExpressionEncoder<Row> encoder) {
    return sparkSession.emptyDataset(encoder);
  }

  private Dataset<Row> decodeWithAvroSchema(Dataset<Row> df, AvroConverter avroConverter, ExpressionEncoder<Row> encoder, Task task) {
    return df.flatMap(
        (FlatMapFunction<Row, Row>) row -> avroConverter
            .decode(task.getSchema(), row.getAs("value"))
            .iterator()
        , encoder
    );
  }
}

