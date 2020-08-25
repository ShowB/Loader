package com.snet.smore.loader.domain;

import lombok.Data;

import java.util.List;

@Data
public class InsertInfo {
    private String query;
    private List<String> columns;
}
