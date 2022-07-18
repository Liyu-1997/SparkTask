package com.sg.java;

import com.google.gson.Gson;

public class JSONUtil {

    private static final Gson gson = new Gson();

    public static Gson getInstance() {
        return gson;
    }

    public static String toJson(Object src) {
        return gson.toJson(src);
    }

    public static <T> T fromJson(String json, Class<T> typeOfT) {
        return gson.fromJson(json, typeOfT);
    }

}
