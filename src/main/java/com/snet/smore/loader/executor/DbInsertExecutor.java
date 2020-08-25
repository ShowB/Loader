package com.snet.smore.loader.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snet.smore.common.constant.Constant;
import com.snet.smore.common.constant.FileStatusPrefix;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.FileUtil;
import com.snet.smore.loader.main.LoaderMain;
import com.snet.smore.loader.module.DbInsertModule;
import com.snet.smore.loader.util.ListCollection;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.Callable;

@Slf4j
public class DbInsertExecutor implements Callable<String> {

    private Path path;

    public DbInsertExecutor(Path p) {
        this.path = p;
    }

    @Override
    public String call() {
        try {
            path = FileUtil.changeFileStatus(path, FileStatusPrefix.TEMP);
        } catch (IOException e) {
            log.error("File is using by another process. {}", path);
            return Constant.CALLABLE_RESULT_FAIL;
        }

        int maxCount = EnvManager.getProperty("loader.target.db.commit-size", 1000) * 2;

        ObjectMapper mapper = new ObjectMapper();

        String line;

        try (FileInputStream fis = new FileInputStream(path.toFile());
             ReadableByteChannel rbc = Channels.newChannel(fis);
             Reader channelReader = Channels.newReader(rbc, "UTF-8");
             BufferedReader br = new BufferedReader(channelReader)) {

            while ((line = br.readLine()) != null) {
                ListCollection.LOAD_QUEUE.add(mapper.readValue(line, JSONObject.class));

                while (ListCollection.LOAD_QUEUE.size() >= maxCount
                        || ListCollection.ERROR_QUEUE.size() >= maxCount) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        log.error("An error occurred while thread sleeping.", e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("An error occurred while reading file. {}", path, e);
            try {
                path = FileUtil.changeFileStatus(path, FileStatusPrefix.ERROR);
            } catch (IOException ex) {
                log.error("An error occurred while renaming file name. {}", path, e);
            }
        }

        try {
            path = FileUtil.changeFileStatus(path, FileStatusPrefix.COMPLETE);
        } catch (IOException e) {
            log.error("An error occurred while renaming file name. {}", path, e);
        }

        System.out.println("[" + LoaderMain.getNextCnt() + " / " + LoaderMain.getTotalCnt() + "]" + "\tFile read completed.\t" + path);

        return Constant.CALLABLE_RESULT_SUCCESS;
    }
}
