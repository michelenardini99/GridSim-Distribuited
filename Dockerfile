# Stage 1: Build the application distribution
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /workspace

# Copy Gradle wrapper and configuration first for layer caching
COPY gradlew .
COPY gradle gradle
COPY gradle.properties settings.gradle.kts build.gradle.kts ./
COPY protocols/build.gradle.kts protocols/build.gradle.kts
COPY app/build.gradle.kts app/build.gradle.kts

RUN chmod +x gradlew

# Copy source code
COPY protocols/src protocols/src
COPY app/src app/src

# Build unpacked distribution (preserves Pekko reference.conf files across all jars)
RUN ./gradlew :app:installDist --no-daemon -x test

# Stage 2: Lightweight runtime image
FROM eclipse-temurin:21-jre

WORKDIR /app

# Copy the installed distribution from builder stage
COPY --from=builder /workspace/app/build/install/app /app

EXPOSE 2551 8080

ENTRYPOINT ["/app/bin/app", "backend-cluster"]

