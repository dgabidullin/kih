package net.dag.kih.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class JsonConfig {

  @Bean
  ObjectMapper getInstanceObjectMapper() {
    ObjectMapper mapper = new ObjectMapper().registerModule(
        new JavaTimeModule()
    ).configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setAnnotationIntrospector(new JacksonAnnotationIntrospector() {
      @Override
      public JsonPOJOBuilder.Value findPOJOBuilderConfig(AnnotatedClass ac) {
        if (ac.hasAnnotation(JsonPOJOBuilder.class)) {//If no annotation present use default as empty prefix
          return super.findPOJOBuilderConfig(ac);
        }
        return new JsonPOJOBuilder.Value("build", "");
      }
    });
    return mapper;
  }
}
