package com.snet.smore.loader.main;

import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.FileUtil;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileInit {
    public static void main(String[] args) {
        int i = 1;

        if (i == 0) {
            FileUtil.initFiles(Paths.get(EnvManager.getProperty("loader.source.file.dir")));
        } else if (i == 1) {
            for (int j = 0; j < 10; j++) {
                FileUtil.initFiles(Paths.get("D:/SMORE_DATA/LOADER_SOURCE/TC_KORAIL_C" + j));
            }
        }

    }
}