FROM openjdk:8-jdk-alpine AS target

WORKDIR /build
COPY .mvn /build/.mvn
COPY ./pom.xml ./mvnw /build/
RUN ./mvnw dependency:go-offline

COPY src/ /build/src/
RUN ./mvnw package

FROM openjdk:8-jre-slim
COPY --from=target /build/target/dependency-jars /run/dependency-jars
COPY --from=target /build/target/application.jar /run/application.jar
ENTRYPOINT java -jar run/application.jar -D exec.mainClass="com.github.anishitani.kafka.producer.TwitterProducer"