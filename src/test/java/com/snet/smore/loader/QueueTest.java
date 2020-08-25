package com.snet.smore.loader;

import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

public class QueueTest {
    @Test
    public void main() {
        ConcurrentLinkedQueue<String> que = new ConcurrentLinkedQueue<>();

//        que.add("a");
//        que.add("b");
//
//        while (!que.isEmpty()) {
//            System.out.println(que.remove());
//        }
        ArrayList<String> list1 = new ArrayList<>();
        ArrayList<String> list2 = new ArrayList<>();

        Thread t1 = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                int v = (int) (Math.random() * 1000);
                que.add("" + v);
//                System.out.println("add " + v);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread t2 = new Thread(() -> {
            for (int i = 100; i < 200; i++) {
                int v = (int) (Math.random() * 1000);
                que.add("" + v);
//                System.out.println("add " + v);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread t3 = new Thread(() -> {
            while (list1.size() < 100) {
                while (!que.isEmpty()) {
                    String poll = que.poll();
                    list1.add(poll);
                    System.out.println("3's poll\t" + poll);
                }

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread t4 = new Thread(() -> {
            while (list2.size() < 100) {
                while (!que.isEmpty()) {
                    String poll = que.poll();
                    list2.add(poll);
                    System.out.println("4's poll\t" + poll);
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        t1.start();
        t2.start();
        t3.start();
        t4.start();

        while (t3.isAlive() || t4.isAlive()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (String s : list1) {
            if (list2.contains(s))
                System.out.println("here!!!!!");
        }


    }

    @Test
    @Ignore
    public void test2() {
        ConcurrentLinkedQueue<Map<String, Object>> queue = new ConcurrentLinkedQueue();

        Map<String, Object> map1 = new HashMap<>();
        List<Map<String, Object>> list1 = new LinkedList<>();
        Map<String, Object> subMap1 = new HashMap<>();
        subMap1.put("UUID", UUID.randomUUID());
        Map<String, Object> subMap2 = new HashMap<>();
        subMap2.put("UUID", UUID.randomUUID());
        list1.add(subMap1);
        list1.add(subMap2);

        map1.put("table1", list1);
        queue.add(map1);

        Map<String, Object> secMap1 = new HashMap<>();
        List<Map<String, Object>> secList1 = new LinkedList<>();
        Map<String, Object> secSubMap1 = new HashMap<>();
        secSubMap1.put("UUID", UUID.randomUUID());
        Map<String, Object> secSubMap2 = new HashMap<>();
        secSubMap2.put("UUID", UUID.randomUUID());
        secList1.add(secSubMap1);
        secList1.add(secSubMap2);

        secMap1.put("table1", secList1);
        queue.add(secMap1);

        System.out.println(queue);
    }
}
