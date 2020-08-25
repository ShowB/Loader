package com.snet.smore.loader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import java.io.IOException;

public class JsonTest {
    @Test
    public void test() throws IOException {
        JSONObject json = new JSONObject();
        json.put("test1", '0');
        json.put("test2", '1');
        json.put("test3", "0");
        json.put("test4", 0.030);
        json.put("test5", 1.0501);

        System.out.println(json.toString());
        System.out.println(json.get("test2").getClass());

        System.out.println(JSONObject.toJSONString(json));

        Gson gson = new Gson();

        JSONObject jsonObject = gson.fromJson(json.toString(), JSONObject.class);

        System.out.println(jsonObject);

        ObjectMapper mapper = new ObjectMapper();
        JSONObject jsonObject1 = mapper.readValue(json.toString(), JSONObject.class);

        System.out.println(jsonObject1.toString());


    }
}
