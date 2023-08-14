package org.pentaho.di.trans.steps.starrockskettleconnector;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarRocksKettleConnectorWriterTest {
    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();
    StarRocksKettleConnectorMeta lmeta;
    StarRocksKettleConnectorData ldata;
    StarRocksKettleConnector lder;

    RowMeta rm=new RowMeta();
    StepMeta smeta;

    @BeforeClass
    public static void initEnvironment() throws Exception {
        KettleEnvironment.init();
    }

    @Before
    public void setUp() {
        TransMeta transMeta = new TransMeta();
        transMeta.setName("StarRocksKettleConnector");

        Map<String, String> vars = new HashMap<>();
        vars.put("loadurl", "10.112.133.149:8040");
        vars.put("jdbcurl", "jdbc:mysql://10.112.133.149:9030");
        vars.put("databasename", "kettle_test");
        vars.put("tablename", "student");
        vars.put("user", "root");
        vars.put("password", "");
        vars.put("format", "CSV");
        transMeta.injectVariables(vars);

        lmeta = new StarRocksKettleConnectorMeta();
        List<String> loadurl = Arrays.asList(vars.get("loadurl").split(";"));
        lmeta.setLoadurl(loadurl);
        lmeta.setJdbcurl(transMeta.environmentSubstitute("${jdbcurl}"));
        lmeta.setDatabasename(transMeta.environmentSubstitute("${databasename}"));
        lmeta.setTablename(transMeta.environmentSubstitute("${tablename}"));
        lmeta.setUser(transMeta.environmentSubstitute("${user}"));
        lmeta.setPassword(transMeta.environmentSubstitute("${password}"));
        lmeta.setFormat(transMeta.environmentSubstitute("${format}"));
        lmeta.setMaxbytes(12);
        lmeta.setScanningFrequency(50);
        lmeta.setConnecttimeout(1000);
        lmeta.setTimeout(600);
        lmeta.setMaxFilterRatio(0);

        lmeta.setFieldStream(new String[]{"id","name","sorce"});
        lmeta.setFieldTable(new String[]{"id","name","sorce"});
        ValueMetaInteger vi=new ValueMetaInteger("id");
        ValueMetaString vs=new ValueMetaString("name");
        ValueMetaNumber vn=new ValueMetaNumber("sorce");
        rm.addValueMeta(vi);
        rm.addValueMeta(vs);
        rm.addValueMeta(vn);

        ldata = new StarRocksKettleConnectorData();
        PluginRegistry plugReg = PluginRegistry.getInstance();
        String skcPid = plugReg.getPluginId(StepPluginType.class, lmeta);
        smeta = new StepMeta(skcPid, "StarRocksKettleConnector", lmeta);
        Trans trans = new Trans(transMeta);
        transMeta.addStep(smeta);
        lder = new StarRocksKettleConnector(smeta, ldata, 1, transMeta, trans);
        lder.setInputRowMeta(rm);
        lder.copyVariablesFrom(transMeta);

    }

    @Test
    public void testStreamLoad() throws KettleException{
        //lder.init(lmeta,ldata);
//        byte[] id=new String("1").getBytes();
//        byte[] name=new String("Lili").getBytes();
//        byte[] sorce=new String("89.2").getBytes();
//        lder.putRow(rm,new Object[]{id,name,sorce});

        //lder.processRow(lmeta,ldata);
    }
}
