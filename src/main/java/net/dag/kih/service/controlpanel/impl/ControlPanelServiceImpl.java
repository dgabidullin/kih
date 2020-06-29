package net.dag.kih.service.controlpanel.impl;

import io.micrometer.core.annotation.Timed;
import lombok.extern.slf4j.Slf4j;
import net.dag.kih.configuration.kafka.KafkaInputHandlerProperties;
import net.dag.kih.service.controlpanel.ControlPanelService;
import net.dag.kih.model.TaskResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class ControlPanelServiceImpl implements ControlPanelService<TaskResult> {

  private static final long serialVersionUID = 1L;

  private final KafkaTemplate<String, TaskResult> template;

  private final KafkaInputHandlerProperties properties;

  @Autowired
  public ControlPanelServiceImpl(KafkaTemplate<String, TaskResult> template, KafkaInputHandlerProperties properties) {
    this.properties = properties;
    this.template = template;
  }

  @Override
  @Timed("psaTask.resultSending")
  public CompletableFuture<SendResult<String, TaskResult>> send(TaskResult taskResult) {
    log.info("[{}] Sending result for task. Status: {}", taskResult.getTask().getId(), taskResult.getStatus());
    return template.send(properties.getPsaTaskResultProducerProps().getTopicName(), taskResult).completable();
  }
}
