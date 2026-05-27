#!/usr/bin/env bash
set -euo pipefail

# Downloads the Prometheus JMX exporter javaagent that the brokers load via KAFKA_OPTS.
# The jar is gitignored (binary), so run this once before the first `docker compose up`.

VERSION="1.0.1"
DEST_DIR="$(cd "$(dirname "$0")/.." && pwd)/compose/jmx"
JAR="${DEST_DIR}/jmx_prometheus_javaagent-${VERSION}.jar"
URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${VERSION}/jmx_prometheus_javaagent-${VERSION}.jar"

if [[ -f "$JAR" ]]; then
    echo "Already present: $JAR"
    exit 0
fi

echo "Fetching jmx_prometheus_javaagent ${VERSION} -> ${JAR}"
curl -fsSL -o "$JAR" "$URL"
echo "Done."
