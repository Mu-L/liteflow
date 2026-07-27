#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

require_passing_report() {
  local report="$1"
  test -f "$report"
  grep -Eq 'Tests run: [1-9][0-9]*, Failures: 0, Errors: 0, Skipped: 0' "$report"
}

mvn -pl liteflow-rule-db/liteflow-rule-db-publisher,liteflow-testcase-el/liteflow-testcase-el-rule-db-publisher \
  clean verify -DskipTests=false -Dmaven.test.skip=false

reports="liteflow-testcase-el/liteflow-testcase-el-rule-db-publisher/target/surefire-reports"
require_passing_report "$reports/com.yomahub.liteflow.publisher.RulePublisherEdgeCaseTest.txt"
require_passing_report "$reports/com.yomahub.liteflow.publisher.RulePublisherServiceLoaderTest.txt"

mvn -pl liteflow-rule-db/liteflow-rule-db-publisher,liteflow-testcase-el/liteflow-testcase-el-rule-db-publisher \
  org.jacoco:jacoco-maven-plugin:0.8.12:prepare-agent test \
  org.jacoco:jacoco-maven-plugin:0.8.12:report-aggregate \
  -DskipTests=false -Dmaven.test.skip=false

coverage_csv="liteflow-testcase-el/liteflow-testcase-el-rule-db-publisher/target/site/jacoco-aggregate/jacoco.csv"
test -f "$coverage_csv"
awk -F, '
  NR > 1 && $1 ~ /liteflow-rule-db-publisher$/ {
    branch_missed += $6; branch_covered += $7;
    line_missed += $8; line_covered += $9;
  }
  END {
    branch_total = branch_missed + branch_covered;
    line_total = line_missed + line_covered;
    branch_rate = branch_total == 0 ? 0 : 100 * branch_covered / branch_total;
    line_rate = line_total == 0 ? 0 : 100 * line_covered / line_total;
    printf "liteflow-rule-db-publisher coverage: lines %.2f%%, branches %.2f%%\n", line_rate, branch_rate;
    if (line_rate < 80 || branch_rate < 60) exit 1;
  }
' "$coverage_csv"
