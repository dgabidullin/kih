package net.dag.kih.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaSource implements Serializable {

  private static final long serialVersionUID = 1L;

  private String topic;

  private Map<String, Map<String, Long>> startingOffsets = new HashMap<>();

  private Map<String, Map<String, Long>> endingOffsets = new HashMap<>();
}
