package com.lin.gmall.hive.udft;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.json.JSONArray;


/**
 * GenericUDTFCount2 outputs the number of rows seen, twice. It's output twice
 * to test outputting of rows on close with lateral view.
 *
 */
public class ExplodeJSONArray extends GenericUDTF {

    Integer count = Integer.valueOf(0);
    Object forwardObj[] = new Object[1];

    @Override
    public void close() throws HiveException {
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 1) {
            throw new UDFArgumentException("explode_json_array 只需要一个参数");
        }

        if(argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentException("explode_json_array 只接受基础数据类型");
        }

        PrimitiveObjectInspector a1 = (PrimitiveObjectInspector) argOIs[0];

        if(a1.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentException("explode_json_array 只接受string类型参数");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("items");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String jsonArray = args[0].toString();

        JSONArray actions = new JSONArray(jsonArray);

        for (int i = 0; i < actions.length(); i++) {
            String[] result = new String[1];
            result[0] = actions.getString(i);
            forward(result);
        }
    }

}