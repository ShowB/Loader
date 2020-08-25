package com.snet.smore.loader;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class EtcTest {
    @Test
    public void test() {
        String a = "jdbc:mariadb://192.168.20.41:3306/SMART";

        System.out.println(a.substring(a.lastIndexOf("/") + 1));
    }

    @Test
    public void test2() {
        List<String> list = new LinkedList<>();
        list.add("gdsfg");
        list.add("5");
        list.add("0");
        list.add("a");

        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i));
        }

        System.out.println("*****");

        for (String s : list) {
            System.out.println(s);
        }
    }
}
