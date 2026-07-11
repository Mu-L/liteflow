/**
 * <p>Title: liteflow</p>
 * <p>Description: 轻量级的组件式流程框架</p>
 */
package com.yomahub.liteflow.test.ruledb.redis.cmp;

import com.yomahub.liteflow.core.NodeComponent;
import org.springframework.stereotype.Component;

@Component("a")
public class ACmp extends NodeComponent {

	@Override
	public void process() {
		System.out.println("ACmp executed!");
	}

}
