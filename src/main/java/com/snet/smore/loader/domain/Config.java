package com.snet.smore.loader.domain;

import lombok.Data;

@Data
public class Config {
    private String name;
    private String frameworkDbName;
    private String frameworkDbUrl;
    private String frameworkDbUsername;
    private String frameworkDbPassword;

    private String mode;
    private String sourceFileDir;
    private String sourceFileGlob;

}
