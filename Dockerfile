FROM bde2020/spark-java-template:2.4.3-hadoop2.7

ENV SPARK_APPLICATION_MAIN_CLASS net.dag.kih.KafkaInputHandlerApplication
ENV SPARK_APPLICATION_JAR_NAME kafka-input-handler

COPY template.sh /

RUN apk add --no-cache openjdk8 maven\
      && chmod +x /template.sh \
      && mkdir -p /app \
      && mkdir -p /usr/src/app

# Copy the POM-file first, for separate dependency resolving and downloading
ONBUILD COPY pom.xml /usr/src/app
ONBUILD RUN cd /usr/src/app \
      && mvn dependency:resolve
ONBUILD RUN cd /usr/src/app \
      && mvn verify

# Copy the source code and build the application
ONBUILD COPY . /usr/src/app
ONBUILD RUN cd /usr/src/app \
      && mvn clean package

CMD ["/bin/bash", "/template.sh"]
