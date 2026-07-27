#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

require_passing_report() {
  local report="$1"
  test -f "$report"
  grep -Eq 'Tests run: [1-9][0-9]*, Failures: 0, Errors: 0, Skipped: 0' "$report"
}

mvn -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot \
  -am clean verify -DskipTests=false -Dmaven.test.skip=false

reports="liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot/target/surefire-reports"
require_passing_report "$reports/com.yomahub.liteflow.repository.sql.SqlPollingResilienceTest.txt"
require_passing_report "$reports/com.yomahub.liteflow.repository.sql.SqlContainerIntegrationTest.txt"

command -v jq >/dev/null
jq -e . liteflow-spring-boot-starter/src/main/resources/META-INF/additional-spring-configuration-metadata.json >/dev/null
jq -e . liteflow-spring-boot4-starter/src/main/resources/META-INF/additional-spring-configuration-metadata.json >/dev/null

mvn -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot \
  -am org.jacoco:jacoco-maven-plugin:0.8.12:prepare-agent test \
  org.jacoco:jacoco-maven-plugin:0.8.12:report-aggregate \
  -DskipTests=false -Dmaven.test.skip=false

coverage_csv="liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot/target/site/jacoco-aggregate/jacoco.csv"
test -f "$coverage_csv"
awk -F, '
  NR > 1 && $1 ~ /liteflow-rule-db-sql$/ {
    branch_missed += $6; branch_covered += $7;
    line_missed += $8; line_covered += $9;
  }
  END {
    branch_total = branch_missed + branch_covered;
    line_total = line_missed + line_covered;
    branch_rate = branch_total == 0 ? 0 : 100 * branch_covered / branch_total;
    line_rate = line_total == 0 ? 0 : 100 * line_covered / line_total;
    printf "liteflow-rule-db-sql coverage: lines %.2f%%, branches %.2f%%\n", line_rate, branch_rate;
    if (line_rate < 80 || branch_rate < 60) exit 1;
  }
' "$coverage_csv"

java_specification_version="$(java -XshowSettings:properties -version 2>&1 \
  | awk -F= '/java.specification.version/ {gsub(/ /, "", $2); print $2}')"
if [[ "$java_specification_version" != "1.8" && "${java_specification_version%%.*}" -ge 17 ]]; then
  mvn -pl liteflow-testcase-el/liteflow-testcase-el-rule-db-sql-springboot4 \
    -am clean test -DskipTests=false -Dmaven.test.skip=false
fi
