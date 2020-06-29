package net.dag.kih.service.task.impl;

import io.micrometer.core.annotation.Timed;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.dag.kih.service.psa.PsaService;
import net.dag.kih.service.task.TaskService;
import net.dag.kih.model.Task;
import net.dag.kih.model.TaskResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.Optional;

@Slf4j
@Service
public class TaskServiceImpl implements TaskService {

  private static final long serialVersionUID = 1L;

  private static final int PARTITION_COALESCE = 1;
  private static final String LATEST = "latest";

  private final PsaService<Row, Task> psaService;

  @Autowired
  public TaskServiceImpl(PsaService<Row, Task> psaService) {
    this.psaService = psaService;
  }

  @Override
  @SneakyThrows
  @Timed("psaTask.processing")
  public TaskResult execute(final Task psaTask) {
    return Optional.ofNullable(psaTask).map(task -> {
      log.info("[{}] Writing task", psaTask.getId());
      String destPath = psaTask.getParams().getPath();

      log.info("[{}] Processing spark job", psaTask.getId());
      Dataset<Row> dataset = psaService.processDataset(psaTask);

      log.info("[{}] Saving parquet file to path {}", psaTask.getId(), destPath);
      if (task.getKeys().isEmpty()) {
        saveWithLatest(psaTask, destPath, dataset);
      } else {
        psaService.save(dataset, destPath, PARTITION_COALESCE, SaveMode.Overwrite);
      }
      return new TaskResult(psaTask, TaskResult.Status.OK, "");
    }).orElseThrow(() -> new IllegalArgumentException("null psaTask"));
  }

  private void saveWithLatest(Task task, String destPath, Dataset<Row> dataset) {
    int versionIndex = destPath.lastIndexOf("/");
    if (versionIndex == -1) {
      log.warn("[{}] Can't generate latest path for '{}'", task.getId(), destPath);
      psaService.save(dataset, destPath, PARTITION_COALESCE, SaveMode.Overwrite);
      return;
    }
    String latestPath = destPath.substring(0, versionIndex + 1) + LATEST;
    dataset.persist();
    psaService.save(dataset, destPath, PARTITION_COALESCE, SaveMode.Overwrite);
    psaService.save(dataset, latestPath, PARTITION_COALESCE, SaveMode.Overwrite);
    dataset.unpersist();
  }
}
