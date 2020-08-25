package com.snet.smore.loader.main;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.FileUtil;

import java.nio.file.Paths;

public class FileInit {
    public static void main(String[] args) {
        FileUtil.initFiles(Paths.get(EnvManager.getProperty("loader.source.file.dir")));
    }
}