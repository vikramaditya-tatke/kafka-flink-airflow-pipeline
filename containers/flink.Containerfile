FROM docker.io/library/maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY streaming/flink/pom.xml .
COPY streaming/flink/src src
RUN mvn -B -ntp package
FROM docker.io/library/flink:1.20.3-scala_2.12-java17
COPY --from=build --chown=flink:flink /build/target/events.jar /opt/flink/usrlib/events.jar
