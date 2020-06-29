package net.dag.kih.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.Data;
import java.io.Serializable;

@Data
@Builder
@JsonDeserialize(builder = TaskResult.TaskResultBuilder.class)
public class TaskResult implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Task task;

  private final Status status;

  private final String description;

  public TaskResult(Task task, Status status, String description) {
    this.task = task;
    this.status = status;
    this.description = description;
  }

  public enum Status {
    OK,
    ERROR
  }
}
