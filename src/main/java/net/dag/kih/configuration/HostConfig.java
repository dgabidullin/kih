package net.dag.kih.configuration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
@Slf4j
public class HostConfig {

  @Bean
  public String hostname() throws UnknownHostException {
    log.info("Running on host {}", InetAddress.getLocalHost().getHostName());
    return InetAddress.getLocalHost().getHostName();
  }

  @Bean
  public InfoContributor hostnameInfoContributor(String hostname) {
    return builder -> builder.withDetail("hostname", hostname);
  }
}
