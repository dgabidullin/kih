package net.dag.kih.service.spark.writer;

import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public interface DatasetWriter<T extends Row> extends Serializable {

  void write(Dataset<T> dataset, String destPath, int numberOfPartitions, SaveMode saveMode);

}
