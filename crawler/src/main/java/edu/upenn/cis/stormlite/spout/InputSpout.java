package edu.upenn.cis.stormlite.spout;

import java.io.File;

public class InputSpout extends FileSpout {

    @Override
    public String getFilename() {
        File dir = new File(filepath);
        File[] files = dir.listFiles();

        if (files.length != 1) {
            throw new RuntimeException("Unsure which file to use as input file.");
        }

        return files[0].getName();
    }

}
