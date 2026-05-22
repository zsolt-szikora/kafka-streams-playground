#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Update the common.jar in the local .m2 cache"
"$SCRIPT_DIR/../app/mvnw" -f "$SCRIPT_DIR/../app/pom.xml" clean install -pl consumer -am -DskipTests > /dev/null 2>&1

#start the consumer
CLASSPATH_FOR_MODULE="$("$SCRIPT_DIR"/../app/mvnw -f "$SCRIPT_DIR"/../app -pl consumer -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)"

java -cp "$SCRIPT_DIR/../app/consumer/target/classes\
:common/target/classes\
:$CLASSPATH_FOR_MODULE" \
  info.szikora.kafka.playground.consumer.OrderConsumer