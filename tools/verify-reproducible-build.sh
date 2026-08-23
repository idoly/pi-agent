#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT

hash_artifacts() {
  local output=$1
  (
    cd "$root"
    find ai/target core/target vertx/target -maxdepth 1 -type f \
      -name '*.jar' -print0 \
      | LC_ALL=C sort -z \
      | xargs -0 sha256sum
  ) > "$output"
  if [[ $(wc -l < "$output") -ne 9 ]]; then
    echo "Expected three binary/source/Javadoc JARs per module" >&2
    exit 1
  fi
}

cd "$root"
mvn --batch-mode --no-transfer-progress -q -DskipTests clean package
hash_artifacts "$temporary/first.sha256"
mvn --batch-mode --no-transfer-progress -q -DskipTests clean package
hash_artifacts "$temporary/second.sha256"
diff -u "$temporary/first.sha256" "$temporary/second.sha256"
echo "Reproducible artifacts verified:"
cat "$temporary/second.sha256"
