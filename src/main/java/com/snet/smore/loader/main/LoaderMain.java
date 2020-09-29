package com.snet.smore.loader.main;

import com.snet.smore.common.constant.Constant;
import com.snet.smore.common.domain.Agent;
import com.snet.smore.common.util.AgentUtil;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.StringUtil;
import com.snet.smore.loader.domain.DefaultValue;
import com.snet.smore.loader.module.DbInsertModule;
import com.snet.smore.loader.util.InsertUtil;
import com.snet.smore.loader.util.ListCollection;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LoaderMain {
    private String agentType = Constant.AGENT_TYPE_LOADER;
    private String agentName = EnvManager.getProperty("loader.name");

    private boolean isRequiredPropertiesUpdate = true;
    private boolean isFirstRun = true;

    public static Boolean isLoadRunning = false;
    public static Boolean isSecondaryLoadRunning = false;

    public static int success = 0;
    public static int error = 0;

    private Class clazz;
    private Object instance;

    private ScheduledExecutorService mainService = Executors.newSingleThreadScheduledExecutor();
    private ScheduledExecutorService loadService = Executors.newSingleThreadScheduledExecutor();

    private static Integer totalCnt = 0;
    private static Integer currCnt = 0;

    public static Integer getTotalCnt() {
        return totalCnt;
    }

    public static void setTotalCnt(Integer totalCnt) {
        LoaderMain.totalCnt = totalCnt;
    }

    public static Integer getNextCnt() {
        return ++currCnt;
    }

    public static void clearCurrCnt() {
        currCnt = 0;
    }

    public static void main(String[] args) {
        LoaderMain main = new LoaderMain();
        log.info("Agent: [{}:{}] was started !!!", main.agentType, main.agentName);
        main.mainService.scheduleWithFixedDelay(main::runAgent, 1, 1, TimeUnit.SECONDS);
        main.loadService.scheduleWithFixedDelay(main::runLoadQueue, 3, 1, TimeUnit.SECONDS);
    }

    private void runAgent() {
        try {
            final Agent agent = AgentUtil.getAgent(agentType, agentName);

            if (!Constant.YN_Y.equalsIgnoreCase(agent.getUseYn())) {
                return;
            }

            isRequiredPropertiesUpdate = Constant.YN_Y.equalsIgnoreCase(agent.getChangeYn());

            if (isRequiredPropertiesUpdate || isFirstRun) {
                // properties 설정값에 대한 무결성을 보장하기 위해
                // load 작업이 진행중일 땐 새로운 read 작업을 멈춤
                if (isLoadRunning || isSecondaryLoadRunning) {
                    return;
                }

                clazz = null;
                instance = null;

                EnvManager.reload();
                agentName = EnvManager.getProperty("loader.name");

                setDefaultValues();

                log.info("Environment has successfully (re)loaded.");

                if ("Y".equalsIgnoreCase(agent.getChangeYn()))
                    AgentUtil.setChangeYn(agentType, agentName, Constant.YN_N);

                isRequiredPropertiesUpdate = false;
            }

            LoaderMain.clearCurrCnt();

            if (clazz == null) {
                clazz = DbInsertModule.class;
            }

            if (instance == null) {
                instance = clazz.newInstance();
            }

            try {
                clazz.getMethod("execute").invoke(instance);
            } catch (Exception e) {
                log.error("An error occurred while executing DbInsertModule.execute().", e);
            }

            if (isFirstRun)
                isFirstRun = false;

        } catch (Exception e) {
            log.error("An error occurred while thread processing. It will be restarted : {}", e.getMessage());
        }
    }

    private void runLoadQueue() {
        try {
            InsertUtil.load();
            InsertUtil.secondaryLoad();
        } catch (Exception e) {
            log.error("An error occurred while thread processing. It will be restarted : {}", e.getMessage());
        }
    }

    private void setDefaultValues() {
        String defaultValueConfig = EnvManager.getProperty("loader.target.db.default-value");
        String[] defaultValueSet;

        if (StringUtil.isNotBlank(defaultValueConfig)) {
            ListCollection.DEFAULT_VALUES = new ArrayList<>();

            for (String s : defaultValueConfig.split(";")) {
                defaultValueSet = s.split("=");
                ListCollection.DEFAULT_VALUES.add(new DefaultValue(defaultValueSet[0], defaultValueSet[1]));
            }
        }
    }
}