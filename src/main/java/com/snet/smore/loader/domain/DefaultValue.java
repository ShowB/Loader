package com.snet.smore.loader.domain;

import lombok.Data;

@Data
public class DefaultValue {
    private String key;
    private String value;

    public DefaultValue(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
