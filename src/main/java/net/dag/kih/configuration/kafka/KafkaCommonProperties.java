package net.dag.kih.configuration.kafka;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;

@Data
public class KafkaCommonProperties implements Serializable {

  private static final long serialVersionUID = 1L;

  protected String topicName;
  protected String groupId;
  protected int partitionNum;
  protected String bootstrapServers;
  protected String securityProtocol;
  protected Map<String, Object> props;

  public Properties getProperties() {
    Properties properties = new Properties();
    properties.putAll(props);
    return properties;
  }
}
