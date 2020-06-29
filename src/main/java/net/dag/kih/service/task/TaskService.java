package net.dag.kih.service.task;

import net.dag.kih.model.Task;
import net.dag.kih.model.TaskResult;

import java.io.Serializable;


public interface TaskService extends Serializable {
  TaskResult execute(Task task);
}
