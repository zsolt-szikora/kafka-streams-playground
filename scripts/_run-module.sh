#!/usr/bin/env bash
# Compile <module> (and its local deps via -am) into target/classes,
# then run <main-class> with module + common + maven-resolved classpath.
# Usage: _run-module.sh <module> <main-class>
set -euo pipefail

MODULE="${1:?usage: _run-module.sh <module> <main-class>}"
MAIN_CLASS="${2:?usage: _run-module.sh <module> <main-class>}"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
APP_DIR="$SCRIPT_DIR/../app"

echo "Compile $MODULE and its local dependencies (common) into target/classes"
"$APP_DIR/mvnw" -f "$APP_DIR/pom.xml" -pl "$MODULE" -am compile > /dev/null 2>&1

CLASSPATH_FOR_MODULE="$("$APP_DIR/mvnw" -f "$APP_DIR" -pl "$MODULE" -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)" \
  || { echo "mvn dependency:build-classpath failed"; exit 1; }

exec java -cp "$APP_DIR/$MODULE/target/classes:$APP_DIR/common/target/classes:$CLASSPATH_FOR_MODULE" \
  "$MAIN_CLASS"
