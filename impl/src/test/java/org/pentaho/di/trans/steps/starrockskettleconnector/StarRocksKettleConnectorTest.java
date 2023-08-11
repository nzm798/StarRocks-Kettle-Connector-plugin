package org.pentaho.di.trans.steps.starrockskettleconnector;

import com.alibaba.fastjson.JSON;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.row.value.ValueMetaBigNumber;
import org.pentaho.di.core.row.value.ValueMetaNumber;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksDataType;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarRocksKettleConnectorTest {
    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

    StarRocksKettleConnectorMeta lmeta;
    StarRocksKettleConnectorData ldata;
    StarRocksKettleConnector lder;
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
        vars.put("loadurl", "10.112.133.149:8083;10.112.143.215:8083;10.112.156.187:8083");
        vars.put("jdbcurl", "jdbc:mysql://10.112.133.149:9030");
        vars.put("databasename", "somedatabase");
        vars.put("tablename", "sometable");
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
        lmeta.setMaxbytes(94371840);
        lmeta.setConnecttimeout(1000);
        lmeta.setTimeout(600);
        lmeta.setMaxFilterRatio(0);

        ldata = new StarRocksKettleConnectorData();
        PluginRegistry plugReg = PluginRegistry.getInstance();
        String skcPid = plugReg.getPluginId(StepPluginType.class, lmeta);
        smeta = new StepMeta(skcPid, "StarRocksKettleConnector", lmeta);
        Trans trans = new Trans(transMeta);
        transMeta.addStep(smeta);
        lder = new StarRocksKettleConnector(smeta, ldata, 1, transMeta, trans);
        lder.copyVariablesFrom(transMeta);
    }

    @Test
    public void testGetXMLAndLoadXML() throws KettleXMLException {
        lmeta.setFieldTable(new String[0]);

        String xmlString = lmeta.getXML();

        Document document = XMLHandler.loadXMLString("<Step>" + xmlString + "</Step>");

        Node stepNode = (Node) document.getDocumentElement();

        IMetaStore metaStore = null;
        StarRocksKettleConnectorMeta newMeta = new StarRocksKettleConnectorMeta();
        newMeta.loadXML(stepNode, null, metaStore);

        assertEquals(lmeta.getLoadurl(), newMeta.getLoadurl());
        assertEquals(lmeta.getJdbcurl(), newMeta.getJdbcurl());
        assertEquals(lmeta.getDatabasename(), newMeta.getDatabasename());
        assertEquals(lmeta.getTablename(), newMeta.getTablename());
        assertEquals(lmeta.getUser(), newMeta.getUser());
        assertEquals(lmeta.getPassword(), newMeta.getPassword());
        assertEquals(lmeta.getFormat(), newMeta.getFormat());
        assertEquals(lmeta.getMaxbytes(), newMeta.getMaxbytes());
        assertEquals(lmeta.getTimeout(), newMeta.getTimeout());
        assertEquals(lmeta.getConnecttimeout(), newMeta.getConnecttimeout());
        assertEquals(lmeta.getMaxFilterRatio(), newMeta.getMaxFilterRatio(), 0.0001);
    }

    @Test
    public void testVariableSubstitution() throws KettleException {
        lmeta.setFieldTable(new String[0]);
        lder.init(lmeta, ldata);

        assertEquals("somedatabase", ldata.databasename);
        assertEquals("sometable", ldata.tablename);

    }

    @Test
    public void testTypeConvertionForAllTypes() throws KettleException, Exception {

        StarRocksKettleConnector connector = lder;
        ValueMetaInterface mockMeta = mock(ValueMetaBase.class);
        // Test for String
        when(mockMeta.getType()).thenReturn(ValueMetaInterface.TYPE_STRING);
        assertEquals("normalString", connector.typeConvertion(mockMeta, null, "normalString"));
        assertEquals(JSON.parse("{\"test\":\"data\"}"), connector.typeConvertion(mockMeta, StarRocksDataType.JSON, "{\"test\":\"data\"}"));

        // Test for Boolean
        when(mockMeta.getType()).thenReturn(ValueMetaInterface.TYPE_BOOLEAN);
        assertEquals(1L, connector.typeConvertion(mockMeta, null, true));
        assertEquals(0L, connector.typeConvertion(mockMeta, null, false));

        // Test for Integer
        when(mockMeta.getType()).thenReturn(ValueMetaInterface.TYPE_INTEGER);
        assertEquals((byte) 1, connector.typeConvertion(mockMeta, StarRocksDataType.TINYINT, 1L));
        assertEquals((short) 300, connector.typeConvertion(mockMeta, StarRocksDataType.SMALLINT, 300L));
        assertEquals(50000, connector.typeConvertion(mockMeta, StarRocksDataType.INT, 50000L));

        // Test for Number
        RowMeta rm = new RowMeta();
        ValueMetaNumber vm = new ValueMetaNumber("I don't want NPE!");
        rm.addValueMeta(vm);
        assertEquals(100140.123, connector.typeConvertion(rm.getValueMeta(0), null, 100140.123));

        // Test for BigDecimal
        ValueMetaBigNumber vb = new ValueMetaBigNumber("bignumber");
        rm.addValueMeta(vb);
        BigDecimal bigDecimal = new BigDecimal("1000000.123");
        assertEquals(bigDecimal, connector.typeConvertion(rm.getValueMeta(1), null, bigDecimal));

        // Test for Date
        when(mockMeta.getType()).thenReturn(ValueMetaInterface.TYPE_DATE);
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        Date date = dateFormatter.parse("2022-08-05");
        assertEquals("2022-08-05", connector.typeConvertion(mockMeta, null, date));

        // Test for Timestamp
        when(mockMeta.getType()).thenReturn(ValueMetaInterface.TYPE_TIMESTAMP);
        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date dateTime = datetimeFormat.parse("2022-08-05 12:34:56");
        assertEquals("2022-08-05 12:34:56", connector.typeConvertion(mockMeta, null, dateTime));

        // Test for InetAddress
        when(mockMeta.getType()).thenReturn(ValueMetaInterface.TYPE_INET);
        InetAddress address = InetAddress.getByName("www.example.com");
        assertEquals("93.184.216.34", connector.typeConvertion(mockMeta, null, address));
    }

}
