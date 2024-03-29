package org.pentaho.di.trans.steps.starrockskettleconnector.starrocks;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class StarRocksJsonSerializer implements StarRocksISerializer{
    private final String[] fieldNames;
    public StarRocksJsonSerializer(String[] fieldNames){
        this.fieldNames=fieldNames;
    }
    @Override
    public String serialize(Object[] values) {
        Map<String,Object>rowMap=new LinkedHashMap<>(values.length);
        int idx=0;
        for (String fieldName:fieldNames){
            rowMap.put(fieldName,values[idx]);
            idx++;
        }
        return JSON.toJSONString(rowMap);
    }
}
