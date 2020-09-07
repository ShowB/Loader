package com.snet.smore.loader.executor;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Callable;

@Slf4j
public class DbInsertExecutor implements Callable<String> {

    private Path path;
    private String tableName;

    public DbInsertExecutor(Path p, String tableName) {
        this.path = p;
        this.tableName = tableName;
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
        String sourceFileType = EnvManager.getProperty("loader.source.file.type", "json");

        ObjectMapper mapper = new ObjectMapper();
        JSONObject row;

        if ("json".equalsIgnoreCase(sourceFileType)) {
            byte b;
            byte[] bs;
            Stack<Byte> stack = new Stack();
            int rowStart;
            int rowEnd;
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                ByteBuffer buffer = ByteBuffer.allocate((int) Files.size(path));
                channel.read(buffer);
                channel.close();

                buffer.flip();

                while (buffer.position() < buffer.limit()) {
                    while (ListCollection.LOAD_QUEUE.size() >= maxCount
                            || ListCollection.ERROR_QUEUE.size() >= maxCount) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            log.error("An error occurred while thread sleeping.", e);
                        }
                    }

                    b = buffer.get();

                    if (b == '[' && stack.size() == 0) {
                        b = buffer.get();
                    }

                    if (b == '{') {
                        stack.push(b);

                        if (stack.size() == 1) {
                            buffer.mark();
                        }
                    }

                    if (b == '}') {
                        stack.pop();

                        if (stack.size() == 0) {
                            rowEnd = buffer.position();
                            buffer.reset();
                            buffer.position(buffer.position() - 1);
                            rowStart = buffer.position();

                            bs = new byte[rowEnd - rowStart];

                            buffer.get(bs, 0, rowEnd - rowStart);
                            row = mapper.readValue(bs, JSONObject.class);
                            row.put(Constant.COMMON_COLUMN_SMORE_TABLE_NM, tableName);
                            ListCollection.LOAD_QUEUE.add(row);
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

                return Constant.CALLABLE_RESULT_FAIL;
            }
        } else if ("csv".equalsIgnoreCase(sourceFileType)) {

            try (FileInputStream fis = new FileInputStream(path.toFile());
                 InputStreamReader isr = new InputStreamReader(fis, EnvManager.getProperty("loader.source.file.encoding", "UTF-8"));
                 CSVReader csvReader = new CSVReader(isr
                         , EnvManager.getProperty("loader.source.file.csv.separator", ',')
                         , EnvManager.getProperty("loader.source.file.csv.quote", CSVWriter.NO_QUOTE_CHARACTER))) {

                String[] line;
                int lineCnt = 0;
                row = new JSONObject();
                List<String> keys = new ArrayList<>();

                while ((line = csvReader.readNext()) != null) {
                    while (ListCollection.LOAD_QUEUE.size() >= maxCount
                            || ListCollection.ERROR_QUEUE.size() >= maxCount) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            log.error("An error occurred while thread sleeping.", e);
                        }
                    }

                    if (lineCnt == 0) {
                        keys.addAll(Arrays.asList(line));
                    } else {
                        row.clear();
                        for (int i = 0; i < line.length; i++) {
                            try {
                                row.put(keys.get(i), "NULL".equalsIgnoreCase(line[i]) ? null : line[i]);
                            } catch (Exception ex) {
                                log.error(ex.getMessage() + " {}", lineCnt);
                            }
                        }

                        row.put(Constant.COMMON_COLUMN_SMORE_TABLE_NM, tableName);
                        ListCollection.LOAD_QUEUE.add((JSONObject) row.clone());
                    }

                    lineCnt++;
                }

                csvReader.close();
                isr.close();
                fis.close();

            } catch (Exception e) {
                log.error("An error occurred while reading file. {}", path, e);
                try {
                    path = FileUtil.changeFileStatus(path, FileStatusPrefix.ERROR);
                } catch (IOException ex) {
                    log.error("An error occurred while renaming file name. {}", path, e);
                }

                return Constant.CALLABLE_RESULT_FAIL;
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
