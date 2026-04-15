# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jdk AS build
WORKDIR /workspace

COPY pom.xml .
COPY data-mapper/pom.xml data-mapper/pom.xml
COPY data-mapper/src data-mapper/src
COPY adapter/pom.xml adapter/pom.xml
COPY adapter/src adapter/src

RUN --mount=type=cache,target=/root/.m2 \
    apt-get update && apt-get install -y --no-install-recommends maven && \
    mvn --batch-mode -DskipTests package

# ── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM eclipse-temurin:25-jre
WORKDIR /app

COPY --from=build /workspace/adapter/target/*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
