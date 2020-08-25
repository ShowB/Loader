package com.snet.smore.loader.util;

import org.json.simple.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ListCollection {
    public static ConcurrentLinkedQueue<JSONObject> LOAD_QUEUE = new ConcurrentLinkedQueue<>();
    public static ConcurrentLinkedQueue<Map<String, List<Map<String, Object>>>> ERROR_QUEUE = new ConcurrentLinkedQueue<>();
}
