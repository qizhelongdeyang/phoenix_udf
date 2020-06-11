package com.itrensheng.phoenix.udf;


import com.google.gson.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;

import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;




/**
 * @author : RandySun (sunfeng152157@sina.com)
 * @date : 2020-06-09  18:30
 * Comment :
 */
@BuiltInFunction(name = GetJsonObjectFunction.NAME, args = {
        @Argument(allowedTypes = {PVarchar.class}),
        @Argument(allowedTypes = {PVarchar.class})})
public class GetJsonObjectFunction extends ScalarFunction {

    private static final Pattern PATTERNKEY = Pattern.compile("([^\\[\\]]+)(\\[([0-9]+|\\*)\\])*?");
    private static final Pattern PATTERNINDEX = Pattern.compile("\\[([0-9]+|\\*)\\]");

    public static final String NAME = "GET_JSON_OBJECT";

    public GetJsonObjectFunction() {}

    public GetJsonObjectFunction(List<Expression> children) throws SQLException {
        super(children);
    }


    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression jsonStringExpression = getChildren().get(0);
        String jsonPath = getJsonPath();

        if (!jsonStringExpression.evaluate(tuple, ptr)) {
            return false;
        }

        if (ptr.getLength() == 0) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }

        if (jsonPath == null || jsonPath.isEmpty() || !jsonPath.startsWith("$")) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }

        String jsonString = (String) PVarchar.INSTANCE.toObject(ptr, jsonStringExpression.getSortOrder());
        Gson gson = new Gson();
        JsonElement extractJson = null;
        try {
            extractJson = gson.fromJson(jsonString, JsonElement.class);
        } catch (JsonParseException e) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return true;
        }

        jsonPath = jsonPath.substring(1);
        String[] pathExpr = jsonPath.split("\\.", -1);
        for (String path : pathExpr) {
            if (extractJson == null) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                return true;
            }
            extractJson = extract(extractJson, path);
        }

        String resultJsonString = null;
        if (extractJson != null) {
            try {
                resultJsonString = gson.toJson(extractJson);
            } catch (Exception e) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                return true;
            }
        }
        ptr.set(PVarchar.INSTANCE.toBytes(resultJsonString));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public String getJsonPath() {
        Expression jsonPathExpression = getChildren().get(1);
        if (jsonPathExpression instanceof LiteralExpression) {
            Object jsonPathValue = ((LiteralExpression) jsonPathExpression).getValue();
            if (jsonPathValue != null) {
                return jsonPathValue.toString();
            }
        }
        return null;
    }

    private JsonElement extract(JsonElement jsonElement, String path) {
        if (jsonElement == null) {
            return null;
        }

        Matcher mKey = PATTERNKEY.matcher(path);
        if (mKey.matches() == Boolean.TRUE) {
            jsonElement = extractKey(jsonElement, mKey.group(1));
            if (jsonElement == null) {
                return null;
            }
        }

        Matcher mIndex = PATTERNINDEX.matcher(path);
        while (mIndex.find()) {
            jsonElement = extractIndex(jsonElement, mIndex.group(1));
            if (jsonElement == null) {
                return null;
            }
        }

        return jsonElement;
    }

    private JsonElement extractKey(JsonElement json, String key) {
        if (json.isJsonObject()) {
            return ((JsonObject) json).get(key);
        } else if (json.isJsonArray()) {
            JsonArray jsonArray = new JsonArray();
            for (int i = 0; i < ((JsonArray) json).size(); i++) {
                JsonElement jsonElement = ((JsonArray) json).get(i);
                if (jsonElement.isJsonObject()) {
                    jsonElement = ((JsonObject) jsonElement).get(key);
                } else {
                    continue;
                }
                if (jsonElement == null) {
                    continue;
                }
                if (jsonElement.isJsonArray()) {
                    for (int j = 0; j < ((JsonArray) jsonElement).size(); j++) {
                        jsonArray.add(((JsonArray) jsonElement).get(i));
                    }
                } else {
                    jsonArray.add(jsonElement);
                }
            }
            if (jsonArray.size() == 0) {
                return null;
            }
            return jsonArray;
        } else {
            return null;
        }
    }

    private JsonElement extractIndex(JsonElement jsonArray, String index) {
        if (jsonArray.isJsonObject()) {
            return null;
        }
        if (index.equals("*")) {
            return jsonArray;
        } else {
            try{
                return ((JsonArray) jsonArray).get(Integer.parseInt(index));
            } catch(IndexOutOfBoundsException e) {
                return null;
            }
        }
    }
}
