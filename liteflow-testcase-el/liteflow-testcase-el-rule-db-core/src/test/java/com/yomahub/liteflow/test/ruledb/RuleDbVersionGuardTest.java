package com.yomahub.liteflow.test.ruledb;

import com.yomahub.liteflow.property.RuleDbConfig;
import com.yomahub.liteflow.repository.RuleDbRuntime;
import com.yomahub.liteflow.repository.vo.ChangeRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * spec §8.5 幂等性回归：applyChange 的 UPSERT 分支必须忽略版本回退的过期变更，
 * 避免一条迟到的旧通知把 per-target 版本戳索引打回低位、进而触发无谓重载。
 * DELETE 分支不受此限（无论版本一律生效）。
 *
 * <p>测试通过直接调用 {@link RuleDbRuntime#applyChange(ChangeRecord)} 驱动，确定性、不依赖调度。
 */
public class RuleDbVersionGuardTest extends BaseRuleDbTest {

	@Test
	public void testUpsertVersionDoesNotRegress() {
		// 引导 Rule-DB 运行时初始化（注册 SPI、建索引、起调度器）
		buildExecutor(new RuleDbConfig());

		// 1. UPSERT chain X version 3 → 版本戳 = 3
		RuleDbRuntime.applyChange(new ChangeRecord(1, ChangeRecord.TargetType.CHAIN, "X", ChangeRecord.Op.UPSERT, 3));
		Assertions.assertEquals(Long.valueOf(3L), RuleDbRuntime.getChainVersion("X"));

		// 2. UPSERT chain X version 2（过期，迟到）→ 必须忽略，版本戳不回退
		RuleDbRuntime.applyChange(new ChangeRecord(2, ChangeRecord.TargetType.CHAIN, "X", ChangeRecord.Op.UPSERT, 2));
		Assertions.assertEquals(Long.valueOf(3L), RuleDbRuntime.getChainVersion("X"),
				"stale change must not regress per-target version index (spec §8.5)");

		// 3. UPSERT chain X version 5（更新）→ 仍可推进
		RuleDbRuntime.applyChange(new ChangeRecord(3, ChangeRecord.TargetType.CHAIN, "X", ChangeRecord.Op.UPSERT, 5));
		Assertions.assertEquals(Long.valueOf(5L), RuleDbRuntime.getChainVersion("X"));
	}

	@Test
	public void testScriptUpsertVersionDoesNotRegress() {
		buildExecutor(new RuleDbConfig());

		// UPSERT script Y version 4
		RuleDbRuntime.applyChange(new ChangeRecord(1, ChangeRecord.TargetType.SCRIPT, "Y", ChangeRecord.Op.UPSERT, 4));
		Assertions.assertEquals(Long.valueOf(4L), RuleDbRuntime.scriptVersionIndex().get("Y"));

		// 过期 version 1 忽略
		RuleDbRuntime.applyChange(new ChangeRecord(2, ChangeRecord.TargetType.SCRIPT, "Y", ChangeRecord.Op.UPSERT, 1));
		Assertions.assertEquals(Long.valueOf(4L), RuleDbRuntime.scriptVersionIndex().get("Y"),
				"stale script change must not regress per-target version index (spec §8.5)");

		// 更新 version 6 推进
		RuleDbRuntime.applyChange(new ChangeRecord(3, ChangeRecord.TargetType.SCRIPT, "Y", ChangeRecord.Op.UPSERT, 6));
		Assertions.assertEquals(Long.valueOf(6L), RuleDbRuntime.scriptVersionIndex().get("Y"));
	}
}
