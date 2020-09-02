package com.snet.smore.loader.main;

import com.snet.smore.common.constant.Constant;
import com.snet.smore.common.domain.Agent;
import com.snet.smore.common.util.AgentUtil;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.StringUtil;
import com.snet.smore.loader.domain.DefaultValue;
import com.snet.smore.loader.module.DbInsertModule;
import com.snet.smore.loader.util.InsertUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LoaderMain {
    private static String agentType = Constant.AGENT_TYPE_LOADER;
    private static String agentName = EnvManager.getProperty("loader.name");

    private static boolean isRequiredPropertiesUpdate = true;
    private static boolean isFirstRun = true;

    public static Boolean isLoadRunning = false;
    public static Boolean isSecondaryLoadRunning = false;

    public static int success = 0;
    public static int error = 0;

    private static ScheduledExecutorService mainService = Executors.newSingleThreadScheduledExecutor();
    private static ScheduledExecutorService loadService = Executors.newSingleThreadScheduledExecutor();

    private static Integer totalCnt = 0;
    private static Integer currCnt = 0;

    public static List<DefaultValue> defaultValues = new ArrayList<>();

    public static Integer getTotalCnt() {
        return totalCnt;
    }

    public static void setTotalCnt(Integer totalCnt) {
        LoaderMain.totalCnt = totalCnt;
    }

    public static Integer getNextCnt() {
        synchronized (currCnt) {
            return ++currCnt;
        }
    }

    public static void clearCurrCnt() {
        synchronized (currCnt) {
            currCnt = 0;
        }
    }

    public static void main(String[] args) {
        log.info("Agent: [{}:{}] was started !!!", agentType, agentName);
        mainService.scheduleWithFixedDelay(LoaderMain::runAgent, 1, 1, TimeUnit.SECONDS);
        loadService.scheduleWithFixedDelay(LoaderMain::runLoadQueue, 3, 1, TimeUnit.SECONDS);
    }

    private static void runAgent() {
        final Agent agent = AgentUtil.getAgent(agentType, agentName);

        if (!Constant.YN_Y.equalsIgnoreCase(agent.getUseYn())) {
            log.info("Agent: [{}:{}] is now sleeping ... [{} + {} = {}]", agentType, agentName, success, error, (success + error));
            return;
        }

        isRequiredPropertiesUpdate = Constant.YN_Y.equalsIgnoreCase(agent.getChangeYn());

        if (isRequiredPropertiesUpdate || isFirstRun) {
            // properties 설정값에 대한 무결성을 보장하기 위해
            // load 작업이 진행중일 땐 새로운 read 작업을 멈춤
            if (isLoadRunning || isSecondaryLoadRunning) {
                return;
            }

            EnvManager.reload();
            agentName = EnvManager.getProperty("loader.name");

            setDefaultValues();

            log.info("Environment has successfully (re)loaded.");

            if ("Y".equalsIgnoreCase(agent.getChangeYn()))
                AgentUtil.setChangeYn(agentType, agentName, Constant.YN_N);

            isRequiredPropertiesUpdate = false;
        }

        LoaderMain.clearCurrCnt();

        try {
            DbInsertModule.execute();
        } catch (Exception e) {
            log.error("An error occurred while executing DbInsertModule.execute().", e);
        }

        if (isFirstRun)
            isFirstRun = false;
    }

    private static void runLoadQueue() {
        try {
            InsertUtil.load();
            InsertUtil.secondaryLoad();
        } catch (Exception e) {
            log.error("An error occurred while executing load().", e);
        }
    }

    private static void setDefaultValues() {
        String defaultValueConfig = EnvManager.getProperty("loader.target.db.default-value");
        String[] defaultValueSet;

        if (StringUtil.isNotBlank(defaultValueConfig)) {
            defaultValues = new ArrayList<>();

            for (String s : defaultValueConfig.split(";")) {
                defaultValueSet = s.split("=");
                defaultValues.add(new DefaultValue(defaultValueSet[0], defaultValueSet[1]));
            }
        }
    }
}