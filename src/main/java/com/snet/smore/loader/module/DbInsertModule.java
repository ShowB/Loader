package com.snet.smore.loader.module;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.FileUtil;
import com.snet.smore.common.util.StringUtil;
import com.snet.smore.loader.executor.DbInsertExecutor;
import com.snet.smore.loader.main.LoaderMain;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
public class DbInsertModule {
    public static void execute() {
        String dirToTableValue = EnvManager.getProperty("loader.target.db.dir-to-table");
        String[] dirToTables;

        if (StringUtil.isNotBlank(dirToTableValue)) {
            dirToTables = dirToTableValue.split(";");
        } else {
            dirToTables = new String[1];
            dirToTables[0] = EnvManager.getProperty("loader.source.file.dir")
                    + "="
                    + EnvManager.getProperty("loader.target.db.table-name");
        }

        long start = System.currentTimeMillis();

        ExecutorService distributeService = Executors.newFixedThreadPool(1);
        List<Callable<String>> callables = new ArrayList<>();

        for (String s : dirToTables) {
            String[] split = s.split("=");

            Path root = Paths.get(split[0]);
            String glob = EnvManager.getProperty("loader.source.file.glob", "*.*");
            List<Path> targets = FileUtil.findFiles(root, glob);

            for (Path p : targets) {
                callables.add(new DbInsertExecutor(p, split[1]));
            }

        }


        LoaderMain.setTotalCnt(callables.size());

        if (callables.size() > 0) {
            log.info("Target files were found: {}", LoaderMain.getTotalCnt());

            try {
                List<Future<String>> futures = distributeService.invokeAll(callables);
                long end = System.currentTimeMillis();
                log.info("{} files have been completed.", futures.size());
                log.info("Turn Around Time: " + ((end - start) / 1000) + " (seconds)");
            } catch (InterruptedException e) {
                log.error("An error occurred while invoking distributed thread.");
            }

        }

//        Path root = Paths.get(EnvManager.getProperty("loader.source.file.dir"));
//        String glob = EnvManager.getProperty("loader.source.file.glob", "*.*");
//        List<Path> targets = FileUtil.findFiles(root, glob);
//
//        if (targets.size() < 1)
//            return;
//
//
//        LoaderMain.setTotalCnt(targets.size());
//        log.info("Target files were found: {}", LoaderMain.getTotalCnt());
//        long start = System.currentTimeMillis();
//
//        ExecutorService distributeService = Executors.newFixedThreadPool(1);
//        List<Callable<String>> callables = new ArrayList<>();
//
//        for (Path p : targets) {
//            callables.add(new DbInsertExecutor(p));
//        }
//
//        try {
//            List<Future<String>> futures = distributeService.invokeAll(callables);
//            long end = System.currentTimeMillis();
//            log.info("{} files have been completed.", futures.size());
//            log.info("Turn Around Time: " + ((end - start) / 1000) + " (seconds)");
//        } catch (InterruptedException e) {
//            log.error("An error occurred while invoking distributed thread.");
//        }
    }
}
