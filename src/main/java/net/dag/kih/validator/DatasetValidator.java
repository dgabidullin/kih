package net.dag.kih.validator;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.Serializable;
import net.dag.kih.exception.DataFormatException;
import org.apache.spark.sql.Dataset;


public interface DatasetValidator<T1, T2> extends Serializable {

  boolean validateNoRecords(T2 psaTask);

  void matchRecordsFromDfWithEventRecords(Dataset<T1> df, T2 task) throws DataFormatException, JsonProcessingException;

  void matchCountAfterDecodeAvroWithEventRecords(Dataset<T1> df, T2 task) throws DataFormatException;

  boolean isEmptyDataset(Dataset<T1> ds);
}
