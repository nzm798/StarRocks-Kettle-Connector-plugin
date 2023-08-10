package org.pentaho.di.trans.steps.starrockskettleconnector;

import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;

import java.util.*;

public class StarRocksKettleConnectorMetaTest {
    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

    public class LoadUrlFieldLoadSaveValidator implements FieldLoadSaveValidator<List<String>> {
        private final int arraySize;

        public LoadUrlFieldLoadSaveValidator(int arraySize) {
            this.arraySize = arraySize;
        }

        @Override
        public List<String> getTestObject() {
            List<String> loadUrlList = new ArrayList<>();
            for (int i = 0; i < arraySize; i++) {
                loadUrlList.add("http://example.com/" + i);
            }
            return loadUrlList;
        }

        @Override
        public boolean validateTestObject(List<String> original, Object actual) {
            if (original == null || actual == null) {
                return original == actual;
            }
            return original.equals(actual);
        }
    }
    @Test
    public void testRoundTrip() throws KettleException {
        List<String> attributes = Arrays.asList("loadurl","jdbcurl", "databasename", "tablename", "user", "password", "format", "maxbytes"
                /*"max_filter_ratio"*/, "connecttimeout", "timeout", "fieldTable", "fieldStream", "partialupdate", "partialcolumns", "enableupsertdelete",
                "upsertordelete");

        Map<String, String> getterMap = new HashMap<>();
        getterMap.put("loadurl", "getLoadurl");
        getterMap.put("jdbcurl", "getJdbcurl");
        getterMap.put("databasename", "getDatabasename");
        getterMap.put("tablename", "getTablename");
        getterMap.put("user", "getUser");
        getterMap.put("password", "getPassword");
        getterMap.put("format", "getFormat");
        getterMap.put("maxbytes", "getMaxbytes");
        //getterMap.put("max_filter_ratio", "getMaxFilterRatio");
        getterMap.put("connecttimeout", "getConnecttimeout");
        getterMap.put("timeout", "getTimeout");
        getterMap.put("fieldTable", "getFieldTable");
        getterMap.put("fieldStream", "getFieldStream");
        getterMap.put("partialupdate", "getPartialUpdate");
        getterMap.put("partialcolumns", "getPartialcolumns");
        getterMap.put("enableupsertdelete", "getEnableUpsertDelete");
        getterMap.put("upsertordelete", "getUpsertOrDelete");

        Map<String, String> setterMap = new HashMap<>();
        setterMap.put("loadurl", "setLoadurl");
        setterMap.put("jdbcurl", "setJdbcurl");
        setterMap.put("databasename", "setDatabasename");
        setterMap.put("tablename", "setTablename");
        setterMap.put("user", "setUser");
        setterMap.put("password", "setPassword");
        setterMap.put("format", "setFormat");
        setterMap.put("maxbytes", "setMaxbytes");
        //setterMap.put("max_filter_ratio", "setMaxFilterRatio");
        setterMap.put("connecttimeout", "setConnecttimeout");
        setterMap.put("timeout", "setTimeout");
        setterMap.put("fieldTable", "setFieldTable");
        setterMap.put("fieldStream", "setFieldStream");
        setterMap.put("partialupdate", "setPartialupdate");
        setterMap.put("partialcolumns", "setPartialcolumns");
        setterMap.put("enableupsertdelete", "setEnableupsertdelete");
        setterMap.put("upsertordelete", "setUpsertOrDelete");

        Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap =
                new HashMap<String, FieldLoadSaveValidator<?>>();
//


        FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator =
                new ArrayLoadSaveValidator<String>(new StringLoadSaveValidator(), 25);

        FieldLoadSaveValidator<List<String>> loadUrlLoadSaveValidator = new LoadUrlFieldLoadSaveValidator(5);
        fieldLoadSaveValidatorAttributeMap.put("loadurl",loadUrlLoadSaveValidator);
        fieldLoadSaveValidatorAttributeMap.put("fieldStream", stringArrayLoadSaveValidator);
        fieldLoadSaveValidatorAttributeMap.put("fieldTable", stringArrayLoadSaveValidator);

        LoadSaveTester loadSaveTester =
                new LoadSaveTester(StarRocksKettleConnectorMeta.class, attributes, getterMap, setterMap,
                        fieldLoadSaveValidatorAttributeMap, new HashMap<String, FieldLoadSaveValidator<?>>());

        loadSaveTester.testSerialization();
    }
}
