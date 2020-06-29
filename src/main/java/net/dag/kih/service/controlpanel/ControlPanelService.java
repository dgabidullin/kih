package net.dag.kih.service.controlpanel;

import org.springframework.kafka.support.SendResult;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

public interface ControlPanelService<T> extends Serializable {
  CompletableFuture<SendResult<String, T>> send(T psaTaskResult);
}
