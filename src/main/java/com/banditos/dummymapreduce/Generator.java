package com.banditos.dummymapreduce;

import java.util.Random;

public class Generator {

    private static final String CHARSET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static Random random = new Random();

    public static String generateWord() {
        StringBuilder sb = new StringBuilder();
        for (int i = random.nextInt(155) + 100; i >= 0; i--) {
            sb.append(CHARSET.charAt(random.nextInt(CHARSET.length())));
        }
        return sb.toString();
    }
}
