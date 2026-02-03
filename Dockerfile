# Stage 1: Build
FROM sbtscala/scala-sbt:graalvm-ce-22.3.3-b1-java17_1.12.1_3.3.7 AS builder

COPY . /app
WORKDIR /app
RUN sbt clean assembly

# Stage 2: Spark 4.0 Runtime
FROM apache/spark:4.0.1-scala2.13-java17-r-ubuntu

USER root
RUN mkdir -p /opt/spark/apps
COPY --from=builder /app/target/scala-2.13/app.jar /opt/spark/apps/app.jar
USER spark