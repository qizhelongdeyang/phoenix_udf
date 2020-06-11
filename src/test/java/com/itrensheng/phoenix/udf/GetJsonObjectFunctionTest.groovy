package com.itrensheng.phoenix.udf

import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.phoenix.expression.Expression
import org.apache.phoenix.expression.LiteralExpression
import org.apache.phoenix.schema.SortOrder
import org.apache.phoenix.schema.types.PVarchar
import org.junit.Test

import java.sql.SQLException

/**
 * @author : RandySun (sunfeng152157@sina.com)
 * @date : 2020-06-11  15:19
 * Comment :
 */

class GetJsonObjectFunctionTest  {


    public static void inputExpression(String jsonString,String jsonPath, String expected, SortOrder order) throws SQLException{
        Expression jsonStringExp = LiteralExpression.newConstant(jsonString,PVarchar.INSTANCE,order);
        Expression jsonPathExp = LiteralExpression.newConstant(jsonPath,PVarchar.INSTANCE);
        List<Expression> expressions = Arrays.<Expression>asList(jsonStringExp, jsonPathExp);
        Expression getJsonObjectFunction = new GetJsonObjectFunction(expressions);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        getJsonObjectFunction.evaluate(null,ptr);
        String result = (String)getJsonObjectFunction.getDataType().toObject(ptr);
        assertEquals(expected, result);
    }

    @Test
    public void testGetJsonObjectFunction() throws SQLException {
        String jsonObject = "{\n" +
                "    \"name\":\"randy\"\n" +
                "}";
        inputExpression(jsonObject,"\$.name", "\"randy\"", SortOrder.ASC);
    }
}
