package com.snet.smore.loader;

import org.junit.Test;

public class RecursiveTest {
    int i = 0;

    @Test
    public void test() {
        System.out.println(++i);

        if (i < 10) {
            test();
        } else {
            System.out.println("The End !!");
        }

    }
}
