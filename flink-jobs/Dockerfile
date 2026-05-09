FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -DskipTests -q && \
    mkdir -p /app/jobs && \
    cp target/cdc-flink-jobs-1.0.0.jar /app/jobs/enriched-orders-job.jar && \
    cp target/cdc-flink-jobs-1.0.0.jar /app/jobs/revenue-aggregates-job.jar && \
    cp target/cdc-flink-jobs-1.0.0.jar /app/jobs/customer-360-job.jar

FROM flink:1.18.1-scala_2.12-java11
WORKDIR /app
COPY --from=build /app/jobs ./jobs
# flink CLI is on PATH in the Flink base image
ENV PATH="/opt/flink/bin:${PATH}"
