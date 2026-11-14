FROM eclipse-temurin:25-jdk AS build
WORKDIR /workspace/backend
COPY backend/mvnw backend/pom.xml ./
COPY backend/.mvn ./.mvn
RUN chmod +x mvnw
COPY backend/src ./src
# Unit tests read ../outputs from the backend module directory.
COPY outputs /workspace/outputs
RUN ./mvnw -q -DskipTests package \
    && cp target/credit-risk-backend-*.jar /workspace/app.jar

FROM eclipse-temurin:25-jre
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app --shell /usr/sbin/nologin app

COPY --from=build /workspace/app.jar /app/app.jar
COPY outputs /app/outputs
COPY ingestion-minio-kafka/data/supplementary /app/supplementary
COPY apache-spark/reports /app/spark-reports

USER app
EXPOSE 8080
ENV SPRING_PROFILES_ACTIVE=prod
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
