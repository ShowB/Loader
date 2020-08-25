package com.snet.smore.loader;

import com.google.gson.Gson;
import com.snet.smore.common.constant.Constant;
import com.snet.smore.common.constant.FileStatusPrefix;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.FileUtil;
import com.snet.smore.loader.util.ListCollection;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LoaderTest {
    @Test
    public void test() {
        ScheduledExecutorService insertService = Executors.newSingleThreadScheduledExecutor();
        insertService.scheduleWithFixedDelay(() -> {
            if (ListCollection.LOAD_QUEUE.size() > 100) {
                for (int i = 0; i < 100; i++) {
                    ListCollection.LOAD_QUEUE.remove();
                    System.out.println("one record removed");
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        Path root = Paths.get(EnvManager.getProperty("loader.source.file.dir"));
        List<Path> targets = FileUtil.findFiles(root, "*.*");

        ByteBuffer buffer;
        boolean isLineEnd = false;
        int curr = 0;

        byte readByte;
        byte[] lineSeparatorBytes = Constant.LINE_SEPARATOR.getBytes();

        Gson gson = new Gson();
        StringBuilder sb = new StringBuilder();

        JSONObject json;
        for (Path p : targets) {
            sb.setLength(0);
            curr = 0;

            try {
                p = FileUtil.changeFileStatus(p, FileStatusPrefix.TEMP);
            } catch (IOException e) {
                log.error("File is using by another process. {}", p);
                continue;
            }

            try (FileChannel channel = FileChannel.open(p, StandardOpenOption.READ)) {
                buffer = ByteBuffer.allocateDirect((int) Files.size(p));
                channel.read(buffer);

                buffer.flip();

                while (buffer.position() < buffer.limit()) {
                    readByte = buffer.get();
                    sb.append((char) readByte);

                    if (buffer.position() == buffer.limit())
                        isLineEnd = true;

                    if (readByte == lineSeparatorBytes[0]) {
                        isLineEnd = true;

                        for (int i = 1; i < lineSeparatorBytes.length; i++) {
                            if (buffer.position() + i < buffer.limit()) {
                                readByte = buffer.get();
                                sb.append((char) readByte);

                                if (readByte != lineSeparatorBytes[i]) {
                                    isLineEnd = false;
                                    break;
                                }

                            }
                        }
                    }

                    if (isLineEnd) {
                        isLineEnd = false;

                        json = gson.fromJson(sb.toString(), JSONObject.class);
                        ListCollection.LOAD_QUEUE.add(json);

                        System.out.println(++curr + " " + json.size());
                        sb.setLength(0);
                    }

                }

                try {
                    p = FileUtil.changeFileStatus(p, FileStatusPrefix.COMPLETE);
                } catch (IOException e) {
                    log.error("An error occurred while renaming file name. {}", p, e);
                }

                System.out.println(ListCollection.LOAD_QUEUE.size());
            } catch (IOException e) {
                log.error("An error occurred while reading file. {}", p, e);
                try {
                    p = FileUtil.changeFileStatus(p, FileStatusPrefix.ERROR);
                } catch (IOException ex) {
                    log.error("An error occurred while renaming file name. {}", p, e);
                }
            }
        }
    }
}
