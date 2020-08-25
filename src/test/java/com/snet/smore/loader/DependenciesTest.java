package com.snet.smore.loader;

import com.snet.smore.common.util.EnvManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class DependenciesTest {

    @Test
    public void test() {
        log.info(EnvManager.getProperty("loader.mode"));
    }
}
