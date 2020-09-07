package com.snet.smore.loader;

import org.json.simple.JSONObject;
import org.junit.Test;

import java.util.Random;
import java.util.Set;
import java.util.Stack;

public class StackTest {
    @Test
    public void test() {
        JSONObject json = new JSONObject();
        json.put("data", "value");
        json.put("tableName", "abc");

        System.out.println(json);
        System.out.println(json.remove("tableName"));
        System.out.println(json);

    }
}
