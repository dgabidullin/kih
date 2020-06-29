package net.dag.kih.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.dag.kih.service.controlpanel.ControlPanelService;
import net.dag.kih.service.task.TaskService;
import net.dag.kih.aspect.DiagnosticOperation;
import net.dag.kih.model.Task;
import net.dag.kih.model.TaskResult;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import javax.validation.Valid;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaskEventListener {

  private final TaskService taskService;
  private final ControlPanelService<TaskResult> controlPanelService;

  @KafkaListener(groupId = "${kafka.psa-task-consumer-props.group-id}", topics = "${kafka.psa-task-consumer-props.topic-name}", containerFactory = "kafkaListenerContainerFactory")
  @DiagnosticOperation
  public void handle(@Valid @Payload Task task,
                     Acknowledgment ack,
                     @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                     @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                     @Header(KafkaHeaders.OFFSET) long offset
  ) {
    log.info("[{}] Getting psaTask: {} from partition {}, topic {}, offset {}, ", task.getId(), task, partition, topic, offset);
    try {
      TaskResult taskResult = taskService.execute(task);
      controlPanelService.send(taskResult)
          .handle((result, exception) -> {
            if (exception != null) {
              log.error("[{}] Exception when sending task result {}", task.getId(), result, exception);
              return -1;
            } else {
              log.info("[{}] Successfully sent task result {} to CP", task.getId(), taskResult);
              return result;
            }
          });
    } catch (Exception e) {
      log.error(e.getMessage());
      TaskResult taskResult = new TaskResult(task, TaskResult.Status.ERROR, e.getMessage());
      controlPanelService
          .send(taskResult)
          .thenAccept(r -> log.info("[{}] Successfully sent task result {}", task.getId(), taskResult));
    } finally {
      ack.acknowledge();
    }
  }
}
