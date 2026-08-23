#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output=${1:-"$root/compat-fixtures/public-api-0.1.0.txt"}
temporary=$(mktemp)
trap 'rm -f "$temporary" "$temporary.classes"' EXIT

artifact() {
  local module=$1
  local prefix=$2
  local -a matches
  mapfile -t matches < <(find "$root/$module/target" -maxdepth 1 -type f \
    -name "$prefix-*.jar" \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar' \
    | LC_ALL=C sort)
  if [[ ${#matches[@]} -ne 1 ]]; then
    echo "Expected exactly one binary JAR for $module; run clean package" >&2
    return 1
  fi
  printf '%s\n' "${matches[0]}"
}

jars=(
  "$(artifact ai pi-agent-ai)"
  "$(artifact core pi-agent-core)"
  "$(artifact vertx pi-agent-vertx)"
)
for jar_file in "${jars[@]}"; do
  if [[ -z "$jar_file" || ! -f "$jar_file" ]]; then
    echo "Build all module JARs before generating the API baseline" >&2
    exit 1
  fi
done
classpath=$(IFS=:; echo "${jars[*]}")

: > "$temporary.classes"
for jar_file in "${jars[@]}"; do
  jar tf "$jar_file" \
    | awk '/[.]class$/ && !/module-info[.]class$/ && !/package-info[.]class$/ {
        sub(/[.]class$/, ""); gsub("/", "."); print
      }' >> "$temporary.classes"
done

{
  echo '# pi-agent public API baseline'
  echo '# Target release: 0.1.0; generated with javap from Java 25 module JARs.'
  echo '# Experimental APIs are listed but may evolve according to docs/api-stability.md.'
  echo
  while IFS= read -r class_name; do
    [[ "$class_name" == *'.internal.'* ]] && continue
    flags=$(javap -classpath "$classpath" -verbose "$class_name" 2>/dev/null \
      | awk '/^[[:space:]]*flags:/{print; exit}')
    [[ "$flags" == *'ACC_PUBLIC'* ]] || continue
    echo "## $class_name"
    javap -classpath "$classpath" -public -constants "$class_name" \
      | sed '/^Compiled from /d'
    echo
  done < <(LC_ALL=C sort -u "$temporary.classes")
} > "$temporary"

mkdir -p "$(dirname "$output")"
mv "$temporary" "$output"
