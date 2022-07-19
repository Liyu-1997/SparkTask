package com.sg.java;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

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

    public static JsonObject toJsonObject(String json) {
        return gson.fromJson(json, JsonObject.class);
    }

}
