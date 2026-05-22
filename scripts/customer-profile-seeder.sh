#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Update the common.jar in the local .m2 cache"
"$SCRIPT_DIR/../app/mvnw" -f "$SCRIPT_DIR/../app/pom.xml" clean install -pl streams -am -DskipTests  > /dev/null 2>&1

#start the seeder
CLASSPATH_FOR_MODULE=""
CLASSPATH_FOR_MODULE="$("$SCRIPT_DIR"/../app/mvnw -f "$SCRIPT_DIR"/../app -pl streams -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)" \
  ||  { echo "mvn dependency:build-classpath failed"; exit 1; }
java -cp "$SCRIPT_DIR/../app/streams/target/classes\
:common/target/classes\
:$CLASSPATH_FOR_MODULE" \
  info.szikora.kafka.playground.streams.CustomerProfileSeeder