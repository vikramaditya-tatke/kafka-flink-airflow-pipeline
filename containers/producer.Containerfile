FROM docker.io/library/eclipse-temurin:17-jdk-jammy AS build
WORKDIR /build
COPY streaming/producer/src/DataProducer.java .
RUN javac -d classes DataProducer.java
FROM docker.io/library/eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /build/classes /app
USER 10001
EXPOSE 9999
ENTRYPOINT ["java", "-cp", "/app", "DataProducer"]
