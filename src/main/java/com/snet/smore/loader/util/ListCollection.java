package com.snet.smore.loader.util;

import com.snet.smore.loader.domain.DefaultValue;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ListCollection {
    public static ConcurrentLinkedQueue<JSONObject> LOAD_QUEUE = new ConcurrentLinkedQueue<>();
    public static ConcurrentLinkedQueue<JSONObject> ERROR_QUEUE = new ConcurrentLinkedQueue<>();
    public static List<DefaultValue> DEFAULT_VALUES = new ArrayList<>();
}
