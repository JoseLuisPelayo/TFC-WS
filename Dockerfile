# syntax=docker/dockerfile:1.7

############################
# 1) Build stage (Maven + JDK 21)
############################
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Cache de dependencias (si tu Docker tiene BuildKit)
COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests dependency:go-offline

# Copia el código y empaqueta
COPY src ./src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests package

############################
# 2) Runtime stage (JRE 21)
############################
FROM eclipse-temurin:21-jre
WORKDIR /app

# Usuario no-root
RUN useradd -r -u 10001 appuser
USER appuser

# Copiar el jar
COPY --from=build /workspace/target/*.jar /app/app.jar

EXPOSE 8080

# Ajustes JVM razonables en contenedor (ajustables)
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:InitialRAMPercentage=25 -Djava.security.egd=file:/dev/./urandom"

ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /app/app.jar"]
