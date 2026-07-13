#!/bin/bash
set -euo pipefail

source hevo_utils.sh

mkdir -p /profiling

# Define ports
RMI_HOST=0.0.0.0

# JVM configuration args
java_args=(
    "-Dcom.sun.management.jmxremote"
    "-Dcom.sun.management.jmxremote.port=${JMX_PORT}"
    "-Dcom.sun.management.jmxremote.rmi.port=${RMI_PORT}"
    "-Djava.rmi.server.hostname=${RMI_HOST}"
    "-Dcom.sun.management.jmxremote.authenticate=false"
    "-Dcom.sun.management.jmxremote.ssl=false"
    "-Dcom.sun.management.jmxremote.local.only=false"
    "-agentlib:jdwp=transport=dt_socket,address=*:5050,server=y,suspend=${DEBUG_SUSPEND:-n}"
    "-Djava.net.preferIPv4Stack=true"
    "-Dlogback.configurationFile=/app/logback.xml"
    "-XX:+UseG1GC"
    "-XX:-HeapDumpOnOutOfMemoryError"
    "-XX:HeapDumpPath=/opt/hevo/neo-connector-service"
    "-XX:+DisableExplicitGC"
    "-XX:+UseTLAB"
    "-XX:+UseCompressedOops"
    # Manifest schema validation needs headroom (mirrors the test-heap note in build.gradle.kts)
    "-Xmx1024m"
    "-Xms512m"
    "-XX:+ShowCodeDetailsInExceptionMessages"
)

# Add JFR profiling args if ENABLE_PROFILING is set
if [ "${ENABLE_PROFILING:-false}" = "true" ]; then
    log_info "JFR profiling enabled"
    java_args+=(
        "-XX:StartFlightRecording=name=MyRecording,filename=/profiling/recording.jfr,dumponexit=true"
        "-XX:+UnlockDiagnosticVMOptions"
        "-XX:+DebugNonSafepoints"
        "-XX:-Inline"
        "-XX:FlightRecorderOptions=stackdepth=256"
        "-XX:+PreserveFramePointer"
    )
fi

# Application args
java_args+=(
    "-jar"
    "connector.jar"
    "-i"
    "${1}"
    "-t"
    "${2}"
    "-c"
    "/app/config.yml"
)

log_info "Starting Connector Service"

java "${java_args[@]}"
