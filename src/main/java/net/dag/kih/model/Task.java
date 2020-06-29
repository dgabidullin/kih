package net.dag.kih.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Task implements Serializable {

  private static final long serialVersionUID = 1L;

  @NotNull
  private UUID id;

  @NotNull
  private String version;

  @NotNull
  private KafkaSource kafkaSource;

  @NotNull
  private Map<String, String> keys = new HashMap<>();

  @NotNull
  private String schema;

  @NotNull
  private Long totalRecords;

  @NotNull
  private Integer totalMessages;

  @NotNull
  private TaskParams params;
}