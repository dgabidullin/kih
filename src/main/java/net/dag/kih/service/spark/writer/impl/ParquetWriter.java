package net.dag.kih.service.spark.writer.impl;

import lombok.extern.slf4j.Slf4j;
import net.dag.kih.service.spark.writer.DatasetWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ParquetWriter implements DatasetWriter<Row> {

  private static final long serialVersionUID = 1L;

  @Override
  public void write(Dataset<Row> dataset, String destPath, int numberOfPartitions, SaveMode saveMode) {
    dataset
        .coalesce(numberOfPartitions)
        .write()
        .mode(saveMode)
        .parquet(destPath);
    log.info("Saved parquet file with destination path={}", destPath);
  }
}
