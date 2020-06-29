package net.dag.kih.validator.impl;

import lombok.extern.slf4j.Slf4j;
import net.dag.kih.exception.DataFormatException;
import net.dag.kih.model.Task;
import net.dag.kih.validator.DatasetValidator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DatasetValidatorImpl<T1 extends Row, T2 extends Task> implements DatasetValidator<T1, T2> {

  @Override
  public boolean validateNoRecords(T2 psaTask) {
    if (psaTask.getTotalMessages() == 0 && psaTask.getTotalRecords() == 0) {
      log.info("[{}] TotalRecords is 0 returns empty dataset", psaTask.getId());
      return true;
    }
    return false;
  }

  @Override
  public void matchRecordsFromDfWithEventRecords(Dataset<T1> df, T2 task) throws DataFormatException {
    long messages = df.count();
    if (messages != task.getTotalMessages()) {
      throw new DataFormatException(
          String.format("messages: expected %d, got %d", messages, task.getTotalMessages()));
    }
  }

  @Override
  public void matchCountAfterDecodeAvroWithEventRecords(Dataset<T1> df, T2 psaTask) throws DataFormatException {
    long records = df.count();
    log.info("[{}] DataFrame count {} and task total records {}", psaTask.getId(), records, psaTask.getTotalRecords());
    if (records != psaTask.getTotalRecords()) {
      throw new DataFormatException(
          String.format("records: expected %d, got %d", records, psaTask.getTotalRecords()));
    }
  }

  @Override
  public boolean isEmptyDataset(Dataset<T1> ds) {
    boolean isEmpty;
    try {
      isEmpty = ((T1[]) ds.head(1)).length == 0;
    } catch (Exception e) {
      return true;
    }
    return isEmpty;
  }
}
