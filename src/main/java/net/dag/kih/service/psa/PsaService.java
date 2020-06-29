package net.dag.kih.service.psa;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.Serializable;


public interface PsaService<T1, T2> extends Serializable {
  Dataset<Row> processDataset(T2 task);

  void save(Dataset<T1> dataset, String destPath, int numberOfPartitions, SaveMode saveMode);
}
