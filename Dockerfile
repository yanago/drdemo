FROM eclipse-temurin:17-jre-alpine AS runtime

RUN apk add --no-cache dumb-init

WORKDIR /app

# Copy shaded jar (built with mvn package -DskipTests)
COPY target/disaster-recovery-replay-1.0.0-SNAPSHOT-all.jar app.jar

# Iceberg warehouse and data dir
ENV ICEBERG_WAREHOUSE=file:///data/warehouse
ENV PORT=8080
EXPOSE 8080

VOLUME /data

ENTRYPOINT ["dumb-init", "--"]
CMD ["java", "-jar", "app.jar"]
