package com.snet.smore.loader;

import com.snet.smore.common.constant.Constant;
import com.snet.smore.common.domain.Agent;
import com.snet.smore.common.util.AgentUtil;
import com.snet.smore.common.util.EnvManager;
import org.junit.Ignore;
import org.junit.Test;

public class AgentTest {
    @Test
    @Ignore
    public void test() {
        final Agent agent = AgentUtil.getAgent(Constant.AGENT_TYPE_LOADER, EnvManager.getProperty("loader.name"));
        System.out.println(agent);
    }
}
