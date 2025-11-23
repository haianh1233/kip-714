# CLAUDE.md - AI Assistant Guide for KIP-714 Project

## Project Overview

This is a reference implementation and testing framework for **KIP-714: Client metrics and observability**, an Apache Kafka Enhancement Proposal that enables clients to push telemetry metrics to brokers using OpenTelemetry protobuf format.

**Project Type**: Java/Gradle application with Docker-based Kafka testing infrastructure
**Primary Purpose**: Testing and demonstrating client metrics push functionality in Kafka 3.7.0+
**Build System**: Gradle with Shadow plugin for fat JAR creation
**Runtime Environment**: Docker Compose with Apache Kafka 3.7.0 in KRaft mode

## Directory Structure

```
/home/user/kip-714/
├── build.gradle                           # Gradle build configuration with shadow plugin
├── settings.gradle                        # Gradle project settings (project name: kip-714)
├── gradlew / gradlew.bat                  # Gradle wrapper scripts
├── docker-compose.yaml                    # Kafka broker orchestration (KRaft mode)
├── README.md                              # User-facing documentation
└── src/main/
    ├── java/io/confluent/cse/
    │   ├── KafkaClientTelemetry.java      # Main broker-side telemetry receiver
    │   └── test/
    │       └── TestJavaClient.java        # Producer client for testing metrics push
    └── resources/
        ├── logback.xml                    # SLF4J/Logback logging configuration
        └── MyTelemetryReceiver.java       # Archived alternative implementation
```

**Note**: No `src/test/` directory exists - testing is manual via Docker and gradle run.

## Key Technologies & Dependencies

### Build System
- **Gradle 7.x+** with Java plugin
- **Shadow plugin 7.1.2** - Creates fat JAR with all dependencies embedded
- **Application plugin** - Main class: `io.confluent.cse.test.TestJavaClient`

### Core Dependencies
- **Apache Kafka Clients 3.7.0** - Kafka producer/consumer APIs and telemetry support
- **OpenTelemetry Proto 0.19.0-alpha** - Protocol buffers for telemetry data format
- **Protobuf Java 3.18.0** - Google Protocol Buffers runtime
- **Logback Classic 1.5.6** - Logging implementation for SLF4J

### Runtime Environment
- **Java 11+** - Source/target compatibility
- **Docker & Docker Compose** - Kafka broker containerization
- **Apache Kafka 3.7.0 (KRaft mode)** - Single-broker cluster for testing

## Architecture & Component Interactions

### System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Docker Broker (KRaft Mode)                     │
│              Apache Kafka 3.7.0 Container                   │
│                                                              │
│  CLASSPATH: /tmp/kip-714-1.0-all.jar (mounted from build)  │
│  KAFKA_METRIC_REPORTERS: io.confluent.cse.KafkaClientTelemetry
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │    KafkaClientTelemetry                                │ │
│  │    (implements ClientTelemetry, MetricsReporter,       │ │
│  │     ClientTelemetryReceiver)                           │ │
│  │                                                         │ │
│  │  exportMetrics(context, payload):                      │ │
│  │    1. Receives OpenTelemetry protobuf payload          │ │
│  │    2. Parses MetricsData from binary payload           │ │
│  │    3. Extracts ResourceMetrics and ScopeMetrics        │ │
│  │    4. Logs client info and metric names/units          │ │
│  └────────────────────────────────────────────────────────┘ │
│           ↑                                                  │
│           │ ClientTelemetryPayload (every 5s via config)    │
└───────────┼──────────────────────────────────────────────────┘
            │
            │ PUSH (not poll) - KIP-714 feature
            │
┌───────────┴──────────────────────────────────────────────────┐
│          TestJavaClient (KafkaProducer)                      │
│          localhost:9092 (PLAINTEXT)                          │
│                                                               │
│  Configuration:                                               │
│    - ENABLE_METRICS_PUSH_CONFIG: true                        │
│    - CLIENT_ID: b69cc35a-7a54-4790-aa69-cc2bd4ee4538        │
│    - LINGER_MS: 10ms                                         │
│    - BATCH_SIZE: 500 bytes                                   │
│    - COMPRESSION_TYPE: snappy                                │
│                                                               │
│  Message Production:                                          │
│    - 50 iterations × 100,000 messages = 5,000,000 total      │
│    - Flush after each batch                                  │
│    - Callback logging for errors                             │
└────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Build Phase**: `gradle shadowJar` creates `/build/libs/kip-714-1.0-all.jar`
2. **Deployment**: Docker Compose mounts JAR to `/tmp/kip-714-1.0-all.jar` in broker container
3. **Broker Startup**: Kafka loads `KafkaClientTelemetry` class from mounted JAR (CLASSPATH)
4. **Client Configuration**: Kafka CLI configures metrics subscription (e.g., `org.apache.kafka.producer.*`, interval 5000ms)
5. **Producer Start**: `TestJavaClient` connects with `ENABLE_METRICS_PUSH=true`
6. **Metrics Collection**: Producer collects internal metrics (throughput, latency, compression ratio, etc.)
7. **Telemetry Push**: Producer serializes metrics to OpenTelemetry protobuf, sends to broker every 5s
8. **Server Processing**: `KafkaClientTelemetry.exportMetrics()` receives payload, parses, and logs
9. **Observation**: Metrics visible in broker container logs (`docker logs broker`)

### Key Classes

#### 1. KafkaClientTelemetry.java
**Location**: `src/main/java/io/confluent/cse/KafkaClientTelemetry.java`
**Role**: Broker-side metrics receiver and reporter

**Implements Three Interfaces**:
- `ClientTelemetry` - Kafka server telemetry endpoint
- `MetricsReporter` - Kafka metrics reporter lifecycle
- `ClientTelemetryReceiver` - Receives client telemetry payloads

**Critical Method**:
```java
public void exportMetrics(AuthorizableRequestContext context,
                          ClientTelemetryPayload payload)
```
- Receives binary OpenTelemetry protobuf data
- Parses using `MetricsData.parseFrom(payload.data())`
- Extracts resource metrics, scope metrics, and individual metrics
- Logs client context (clientId, address, listener) and metric metadata

**Error Handling**: Catches InvalidProtocolBufferException gracefully

#### 2. TestJavaClient.java
**Location**: `src/main/java/io/confluent/cse/test/TestJavaClient.java`
**Role**: Test producer that generates metrics traffic

**Configuration Highlights**:
- Hardcoded client ID: `b69cc35a-7a54-4790-aa69-cc2bd4ee4538`
- Bootstrap server: `localhost:9092`
- Key/value serializers: `StringSerializer`
- Compression: Snappy
- Batching: 500 bytes, 10ms linger

**Production Pattern**:
- 50 outer iterations
- 100,000 messages per iteration
- Flush after each batch
- Callback logging for send errors

## Development Workflows

### Initial Setup

1. **Prerequisites Check**:
   ```bash
   java -version  # Must be Java 11+
   docker --version
   docker-compose --version
   ```

2. **Build the Fat JAR**:
   ```bash
   ./gradlew shadowJar
   ```
   Output: `build/libs/kip-714-1.0-all.jar` (~591K + dependencies)

3. **Start Kafka Broker**:
   ```bash
   docker-compose up -d
   ```
   **Critical**: Broker will FAIL if shadowJar doesn't exist. Error message:
   ```
   org.apache.kafka.common.KafkaException: Class io.confluent.cse.KafkaClientTelemetry cannot be found
   ```

4. **Verify Broker Health**:
   ```bash
   docker logs broker | grep "started (kafka.server.KafkaRaftServer)"
   ```

5. **Configure Client Metrics Subscription**:
   ```bash
   docker exec broker /opt/kafka/bin/kafka-client-metrics.sh \
     --bootstrap-server broker:9092 \
     --alter \
     --name 'basic_producer_metrics' \
     --metrics org.apache.kafka.producer. \
     --interval 5000
   ```

6. **Verify Configuration**:
   ```bash
   docker exec broker /opt/kafka/bin/kafka-client-metrics.sh \
     --bootstrap-server broker:9092 \
     --describe \
     --name "basic_producer_metrics"
   ```
   Expected output:
   ```
   Client metrics configs for basic_producer_metrics are:
     interval.ms=5000
     metrics=org.apache.kafka.producer.
   ```

7. **Run Test Producer**:
   ```bash
   ./gradlew run
   ```

8. **Observe Telemetry Logs**:
   ```bash
   docker logs -f broker | grep "exportMetrics"
   ```
   Expected every 5 seconds:
   ```
   [INFO] ***** KafkaClientTelemetry :: exportMetrics *****
   [INFO] *** Context *** : clientId=..., requestType=..., ...
   ```

### Common Development Tasks

#### Rebuild After Code Changes
```bash
./gradlew clean shadowJar
docker-compose restart
```

#### Access Broker Shell
```bash
docker exec -it broker bash
```

#### Access Broker as Root
```bash
docker exec -u 0 -it broker bash
```

#### View Server Configuration
```bash
docker exec broker cat /etc/kafka/docker/server.properties
```

#### List All Client Metrics Configurations
```bash
docker exec broker /opt/kafka/bin/kafka-client-metrics.sh \
  --bootstrap-server broker:9092 --list
```

#### Clean Up Everything
```bash
docker-compose down -v
./gradlew clean
```

### Testing Workflow

Since this project has no automated tests, testing is manual:

1. **Functional Test**: Verify telemetry data appears in broker logs
2. **Load Test**: Observe metrics under high message volume (5M messages)
3. **Configuration Test**: Try different metric patterns and intervals
4. **Error Test**: Stop broker mid-run, verify producer error handling

## Build & Deployment

### Gradle Tasks

```bash
# Build JAR without running
./gradlew build

# Create fat JAR (required for Docker)
./gradlew shadowJar

# Run test producer
./gradlew run

# Clean build artifacts
./gradlew clean

# List all tasks
./gradlew tasks
```

### Docker Operations

```bash
# Start broker in background
docker-compose up -d

# View logs (follow mode)
docker logs -f broker

# Stop broker
docker-compose stop

# Stop and remove volumes
docker-compose down -v

# Rebuild and restart
docker-compose up -d --build
```

### Port Mappings

- **9092**: Client connections (PLAINTEXT) - maps to `localhost:9092`
- **29092**: Inter-broker communication (PLAINTEXT_HOST)
- **29093**: Controller quorum (CONTROLLER)

## Important Conventions & Patterns

### Code Style
- **Package naming**: `io.confluent.cse` (Confluent Solutions Engineering)
- **Logging**: SLF4J with Logback, INFO level by default
- **Error handling**: Try-catch with log messages, no silent failures
- **Comments**: Minimal - code is self-documenting

### Configuration Patterns

#### Hardcoded Values (Change with Caution)
- Client ID: `b69cc35a-7a54-4790-aa69-cc2bd4ee4538` (in TestJavaClient.java:19)
- Bootstrap server: `localhost:9092` (in TestJavaClient.java:12)
- Message count: 100,000 per batch, 50 batches (in TestJavaClient.java:28-29)

#### Configurable via docker-compose.yaml
- Node ID: `KAFKA_NODE_ID: 1`
- Cluster ID: Auto-generated or set via `CLUSTER_ID`
- Listener ports: Modify `KAFKA_ADVERTISED_LISTENERS`
- Metrics reporter class: `KAFKA_METRIC_REPORTERS`

### Logging Conventions

**Logback Pattern**: `[%date{yyyy-MM-dd HH:mm:ss,SSS}] [%level] %message%n`

**Key Log Markers**:
- `***** KafkaClientTelemetry :: exportMetrics *****` - Telemetry received
- `*** Context ***` - Client metadata
- `*** Payload ***` - Metrics data (can be verbose)

### Naming Conventions

**Gradle Artifacts**: `kip-714-1.0-all.jar` (version from build.gradle)
**Docker Service**: `broker` (not `kafka` - see docker-compose.yaml)
**Metrics Config Name**: `basic_producer_metrics` (convention from README)

## Troubleshooting Guide

### Broker Won't Start

**Symptom**: `Class io.confluent.cse.KafkaClientTelemetry cannot be found`
**Cause**: shadowJar not built or not mounted correctly
**Fix**:
```bash
./gradlew clean shadowJar
docker-compose down
docker-compose up -d
```

### No Telemetry Logs Appearing

**Symptom**: Producer runs but no `exportMetrics` logs in broker
**Possible Causes**:
1. Client metrics not configured - run `kafka-client-metrics.sh --alter`
2. Metrics interval too long - check with `--describe`
3. Producer not pushing metrics - verify `ENABLE_METRICS_PUSH_CONFIG: true`

**Debug**:
```bash
# Check if metrics config exists
docker exec broker /opt/kafka/bin/kafka-client-metrics.sh \
  --bootstrap-server broker:9092 --list

# Check broker API versions support telemetry
docker exec broker /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server broker:9092 | grep GetTelemetrySubscriptions
```

### Gradle Build Fails

**Symptom**: `Could not resolve dependencies`
**Cause**: Network issues with Maven Central or Confluent repository
**Fix**: Check internet connection, wait and retry

### Producer Connection Refused

**Symptom**: `Connection to node -1 (localhost/127.0.0.1:9092) could not be established`
**Cause**: Broker not running or port not exposed
**Fix**:
```bash
docker ps | grep broker  # Verify container running
docker logs broker | tail -50  # Check for startup errors
```

### Out of Memory During Producer Run

**Symptom**: `java.lang.OutOfMemoryError`
**Cause**: Producing 5M messages with default heap
**Fix**: Reduce message count in TestJavaClient.java or increase heap:
```bash
JAVA_OPTS="-Xmx2g" ./gradlew run
```

## AI Assistant Guidelines

### When Modifying Code

1. **Always read files before editing** - Never propose changes to unread code
2. **Preserve existing patterns** - Follow established conventions (SLF4J logging, try-catch style)
3. **Test after changes** - Rebuild shadowJar, restart broker, run producer, check logs
4. **Update documentation** - If behavior changes, update README.md

### When Adding Features

1. **Consider OpenTelemetry compatibility** - This project uses proto v0.19.0-alpha
2. **Maintain Kafka 3.7.0 compatibility** - Don't use APIs from newer versions
3. **Keep fat JAR lean** - Minimize new dependencies
4. **Log meaningfully** - Follow existing log message patterns

### When Debugging

1. **Check broker logs first**: `docker logs broker`
2. **Verify build artifacts**: Ensure `build/libs/kip-714-1.0-all.jar` exists and is recent
3. **Confirm Docker volume mounts**: `docker inspect broker | grep kip-714`
4. **Use Kafka CLI tools**: Don't guess - verify with kafka-client-metrics.sh

### Common Pitfall Avoidance

- **Don't use `kafka` as service name** - It's `broker` in this project
- **Don't forget to rebuild shadowJar** - Code changes require `gradle shadowJar && docker-compose restart`
- **Don't modify hardcoded client ID** - Unless updating corresponding metrics config
- **Don't assume test directory exists** - Create it if adding automated tests
- **Don't use ZooKeeper commands** - This is KRaft mode, ZooKeeper is deprecated

## Git Workflow

**Current Branch**: `claude/claude-md-miblkxscnz5njdex-016cFXfuuhhz3jZCVodvbAmq`

### Commit Guidelines

- **Message style**: Imperative mood (e.g., "Add", "Fix", "Update", not "Added", "Fixed")
- **Scope**: Single logical change per commit
- **Examples from history**:
  - `Added telemetry logging`
  - `Latest changes`
  - `Checkpoint commit - got an initial test demonstrating the entire process`

### Before Committing

1. Rebuild and test: `./gradlew clean shadowJar && docker-compose restart && ./gradlew run`
2. Verify logs: `docker logs broker | grep exportMetrics`
3. Check git status: `git status`

### Pushing Changes

```bash
# Stage changes
git add .

# Commit with descriptive message
git commit -m "Your message here"

# Push to feature branch (with retry logic for network issues)
git push -u origin claude/claude-md-miblkxscnz5njdex-016cFXfuuhhz3jZCVodvbAmq
```

**Network Retry**: If push fails, retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s)

## Quick Reference

### Essential Commands

| Task | Command |
|------|---------|
| Build JAR | `./gradlew shadowJar` |
| Start broker | `docker-compose up -d` |
| Run producer | `./gradlew run` |
| View logs | `docker logs -f broker` |
| Configure metrics | `docker exec broker /opt/kafka/bin/kafka-client-metrics.sh --bootstrap-server broker:9092 --alter --name basic_producer_metrics --metrics org.apache.kafka.producer. --interval 5000` |
| Stop everything | `docker-compose down -v` |

### File Locations

| Purpose | Path |
|---------|------|
| Main telemetry class | `src/main/java/io/confluent/cse/KafkaClientTelemetry.java` |
| Test producer | `src/main/java/io/confluent/cse/test/TestJavaClient.java` |
| Logging config | `src/main/resources/logback.xml` |
| Build config | `build.gradle` |
| Docker config | `docker-compose.yaml` |
| Fat JAR output | `build/libs/kip-714-1.0-all.jar` |

### Key Metrics Configuration

- **Metrics pattern**: `org.apache.kafka.producer.*` (all producer metrics)
- **Interval**: 5000ms (5 seconds)
- **Config name**: `basic_producer_metrics`
- **Client ID filter**: Optional via `--match` parameter

---

**Last Updated**: 2025-11-23
**Project Version**: 1.0
**Kafka Version**: 3.7.0
**Maintainer**: Alex Bleasdale (ableasdale@confluent.io)
