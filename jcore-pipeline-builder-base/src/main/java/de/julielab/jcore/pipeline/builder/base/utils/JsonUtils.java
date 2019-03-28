package de.julielab.jcore.pipeline.builder.base.utils;

import javax.json.JsonArray;
import javax.json.JsonString;
import java.util.ArrayList;
import java.util.List;

public class JsonUtils {

    public static String[] getArrayValuesAsString(JsonArray jarray) {
        List<String> slist = new ArrayList<>();
        for (JsonString s : jarray.getValuesAs(JsonString.class)) {
            slist.add(s.getString());
        }
        String[] sarray = new String[slist.size()];
        return  slist.toArray(sarray);
    }
}
