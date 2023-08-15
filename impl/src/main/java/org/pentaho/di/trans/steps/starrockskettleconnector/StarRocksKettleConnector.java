package org.pentaho.di.trans.steps.starrockskettleconnector;

import com.alibaba.fastjson.JSON;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

public class StarRocksKettleConnector extends BaseStep implements StepInterface {

    private static Class<?> PKG = StarRocksKettleConnectorMeta.class;
    private StarRocksKettleConnectorMeta meta;
    private StarRocksKettleConnectorData data;

    private String logError;

    private long expectDelayTime = 30000L;

    public StarRocksKettleConnector(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (StarRocksKettleConnectorMeta) smi;
        data = (StarRocksKettleConnectorData) sdi;

        try {
            Long a = new Long(1);
            Object[] r = new Object[]{(Object) a, (Object) "Lili", (Object) 19.58};
            //Object[] r = getRow(); // Get row from input rowset & set row busy!
            if (r == null) { // no more input to be expected...
                setOutputDone();
                closeOutput();
                return false;
            }

            if (data.streamLoadManager.getException() != null) {
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Log.AsyncWriteError"), data.streamLoadManager.getException());
                setErrors(1);
                stopAll();
                setOutputDone();
                return false;
            }

            if (first) {
                first = false;

                // Cache field indexes.
                data.keynrs = new int[meta.getFieldStream().length];
                for (int i = 0; i < data.keynrs.length; i++) {
                    data.keynrs[i] = getInputRowMeta().indexOfValue(meta.getFieldStream()[i]);
                }
                data.serializer = getSerializer(meta);
            }

            String serializedValue = data.serializer.serialize(transform(r, meta.getEnableUpsertDelete()));
            data.streamLoadManager.write(null, data.databasename, data.tablename, serializedValue);
            // TODO:实现写入刷新
            putRow(getInputRowMeta(), r);
            incrementLinesOutput();
            return true;

        } catch (Exception e) {
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Log.ErrorInStep"));
            setErrors(1);
            stopAll();
            setOutputDone();
            return false;
        }
    }

    private void closeOutput() throws Exception {
        data.streamLoadManager.flush();
        data.streamLoadManager.close();

        if (data.streamLoadManager.getException() != null) {
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailFlush"), data.streamLoadManager.getException());
        }
        data.streamLoadManager = null;

    }

    // Data type conversion.
    public Object[] transform(Object[] r, boolean supportUpsertDelete) {
        Object[] values = new Object[data.keynrs.length + (supportUpsertDelete ? 1 : 0)];
        for (int i = 0; i < data.keynrs.length; i++) {
            ValueMetaInterface sourceMeta = getInputRowMeta().getValueMeta(data.keynrs[i]);
            StarRocksDataType dataType = data.fieldtype.get(meta.getFieldTable()[i]);
            values[i] = typeConvertion(sourceMeta, dataType, r[i]);
        }
        if (supportUpsertDelete) {
            values[data.keynrs.length] = StarRocksOP.parse(meta.getUpsertOrDelete()).ordinal();
        }
        return values;
    }

    /**
     * Data type conversion.
     *
     * @param sourceMeta
     * @param type
     * @param r
     * @return
     */
    public Object typeConvertion(ValueMetaInterface sourceMeta, StarRocksDataType type, Object r) {
        if (r == null) {
            return null;
        }
        try {
            switch (sourceMeta.getType()) {
                case ValueMetaInterface.TYPE_STRING:
                    // Treat as JSON if it starts with '{' or '['
                    String sValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        sValue = new String((byte[]) r, StandardCharsets.UTF_8);
                    } else {
                        sValue = sourceMeta.getString(r);
                    }
                    if (type == null) {
                        return sValue;
                    }
                    if ((type == StarRocksDataType.JSON ||
                            type == StarRocksDataType.UNKNOWN)
                            && (sValue.charAt(0) == '{' || sValue.charAt(0) == '[')) {
                        return JSON.parse(sValue);
                    } else {
                        return sValue;
                    }
                case ValueMetaInterface.TYPE_BOOLEAN:
                    Boolean boolenaValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        String binaryBoolean = (String) r;
                        boolenaValue = binaryBoolean.equals("1");
                    } else {
                        boolenaValue = sourceMeta.getBoolean(r);
                    }
                    return boolenaValue;
                case ValueMetaInterface.TYPE_INTEGER:
                    Long integerValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        integerValue = Long.parseLong(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        integerValue = sourceMeta.getInteger(r);
                    }
                    if (integerValue >= Byte.MIN_VALUE && integerValue <= Byte.MAX_VALUE && type == StarRocksDataType.TINYINT) {
                        return integerValue.byteValue();
                    } else if (integerValue >= Short.MIN_VALUE && integerValue <= Short.MAX_VALUE && type == StarRocksDataType.SMALLINT) {
                        return integerValue.shortValue();
                    } else if (integerValue >= Integer.MIN_VALUE && integerValue <= Integer.MAX_VALUE && type == StarRocksDataType.INT) {
                        return integerValue.intValue();
                    } else {
                        return integerValue;
                    }
                case ValueMetaInterface.TYPE_NUMBER:
                    Double doubleValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        doubleValue = Double.parseDouble(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        doubleValue = sourceMeta.getNumber(r);
                    }
                    return doubleValue;
                case ValueMetaInterface.TYPE_BIGNUMBER:
                    BigDecimal decimalValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        decimalValue = new BigDecimal(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        decimalValue = sourceMeta.getBigNumber(r);
                    }
                    return decimalValue; // BigDecimal string representation is compatible with DECIMAL
                case ValueMetaInterface.TYPE_DATE:
                    Date dateValue = null;
                    if (sourceMeta.isStorageBinaryString()) {
                        Long milliseconds = Long.parseLong(new String((byte[]) r, StandardCharsets.UTF_8));
                        dateValue = new Date(milliseconds);
                    } else {
                        dateValue = sourceMeta.getDate(r);
                    }
//                    SimpleDateFormat dateFormatter;
//                    if (type == StarRocksDataType.DATE) {
//                        // StarRocks DATE type format: 'yyyy-MM-dd'
//                        dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
//                    } else {
//                        // StarRocks DATETIME type format: 'yyyy-MM-dd HH:mm:ss'
//                        dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                    }
                    return dateValue;
                case ValueMetaInterface.TYPE_TIMESTAMP:
                    java.sql.Timestamp timestampValue = null;
                    if (sourceMeta.isStorageBinaryString()) {
                        Long milliseconds = Long.parseLong(new String((byte[]) r, StandardCharsets.UTF_8));
                        timestampValue = new java.sql.Timestamp(milliseconds);
                    } else {
                        timestampValue = (Timestamp) r;
                    }
//                    SimpleDateFormat timestampFormat;
//                    if (type == StarRocksDataType.DATE) {
//                        // StarRocks DATE type format: 'yyyy-MM-dd'
//                        timestampFormat = new SimpleDateFormat("yyyy-MM-dd");
//                    } else {
//                        // StarRocks DATETIME type format: 'yyyy-MM-dd HH:mm:ss'
//                        timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                    }
                    return timestampValue;
                case ValueMetaInterface.TYPE_BINARY:
                    logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnSupportBinary") + r.toString());
                    return null;

                case ValueMetaInterface.TYPE_INET:
                    String address;
                    if (sourceMeta.isStorageBinaryString()) {

                        address = new String((byte[]) r, StandardCharsets.UTF_8);
                    } else {
                        address = (String) r;
                    }
                    return address;
                default:
                    logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnknowType") + ValueMetaInterface.typeCodes[sourceMeta.getType()]);
                    return null;
            }
        } catch (Exception e) {
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailConvertType") + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (StarRocksKettleConnectorMeta) smi;
        data = (StarRocksKettleConnectorData) sdi;

        if (super.init(smi, sdi)) {
            // Add columns properties to all to prevent changes in the order of the fields.
            if (meta.getPartialUpdate()) {
                data.columns = new String[meta.getPartialcolumns().length];
                System.arraycopy(meta.getPartialcolumns(), 0, data.columns, 0, meta.getPartialcolumns().length);
            } else {
                data.columns = new String[meta.getFieldTable().length];
                System.arraycopy(meta.getFieldTable(), 0, data.columns, 0, meta.getFieldTable().length);
            }
            if (meta.getStarRocksQueryVisitor() == null) {
                // Used to find field information in Starrocks.
                StarRocksJdbcConnectionOptions jdbcConnectionOptions = new StarRocksJdbcConnectionOptions(meta.getJdbcurl(), meta.getUser(), meta.getPassword());
                StarRocksJdbcConnectionProvider jdbcConnectionProvider = new StarRocksJdbcConnectionProvider(jdbcConnectionOptions);
                meta.setStarRocksQueryVisitor(new StarRocksQueryVisitor(jdbcConnectionProvider, meta.getDatabasename(), meta.getTablename()));
            }
            try {
                data.streamLoadManager = new StreamLoadManagerV2(getProperties(meta, data), true);
                data.streamLoadManager.init();
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailConnManager"), e);
                return false;
            }
            try {
                data.fieldtype = meta.getStarRocksQueryVisitor().getFieldMapping();
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.MissingStarRocksFieldType"));
                return false;
            }
            data.tablename = meta.getTablename();
            data.databasename = meta.getDatabasename();
            return true;
        }
        return false;

    }

    public StarRocksISerializer getSerializer(StarRocksKettleConnectorMeta meta) {
        StarRocksISerializer serializer;
        if (meta.getFormat().equals("CSV")) {
            serializer = new StarRocksCsvSerializer(","); // 现在使用默认的，作为列分割符
        } else if (meta.getFormat().equals("JSON")) {
            serializer = new StarRocksJsonSerializer(meta.getFieldTable());
        } else {
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailFormat"));
            return null;
        }
        return serializer;
    }

    // Get the property values needed for Stream Load loading.
    public StreamLoadProperties getProperties(StarRocksKettleConnectorMeta meta, StarRocksKettleConnectorData data) {
        StreamLoadDataFormat dataFormat;
        if (meta.getFormat().equals("CSV")) {
            dataFormat = StreamLoadDataFormat.CSV;
        } else if (meta.getFormat().equals("JSON")) {
            dataFormat = StreamLoadDataFormat.JSON;
        } else {
            throw new RuntimeException("data format are not support");
        }
        StreamLoadTableProperties.Builder defaultTablePropertiesBuilder = StreamLoadTableProperties.builder()
                .database(meta.getDatabasename())
                .table(meta.getTablename())
                .streamLoadDataFormat(dataFormat)
                .enableUpsertDelete(meta.getEnableUpsertDelete());
        if (meta.getPartialUpdate()) {
            defaultTablePropertiesBuilder.addProperty("partial_update", "true");
        }
        // Add the '__op' field
        if (data.columns != null) {
            // don't need to add "columns" header in following cases
            // 1. use csv format but the flink and starrocks schemas are aligned
            // 2. use json format, except that it's loading o a primary key table for StarRocks 1.x
            boolean noNeedAddColumnsHeader;
            if (dataFormat instanceof StreamLoadDataFormat.CSVFormat) {
                noNeedAddColumnsHeader = false;
            } else {
                noNeedAddColumnsHeader = !meta.getEnableUpsertDelete() || meta.isOpAutoProjectionInJson();
            }
            if (!noNeedAddColumnsHeader) {
                String[] headerColumns;
                if (meta.getEnableUpsertDelete()) {
                    headerColumns = new String[data.columns.length + 1];
                    System.arraycopy(data.columns, 0, headerColumns, 0, data.columns.length);
                    headerColumns[data.columns.length] = "__op";
                } else {
                    headerColumns = data.columns;
                }
                String cols = Arrays.stream(headerColumns)
                        .map(f -> String.format("`%s`", f.trim().replace("`", "")))
                        .collect(Collectors.joining(","));
                defaultTablePropertiesBuilder.columns(cols);
            }
        }

        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .labelPrefix("StarRocks-Kettle")
                .loadUrls(meta.getLoadurl().toArray(new String[0]))
                .jdbcUrl(meta.getJdbcurl())
                .defaultTableProperties(defaultTablePropertiesBuilder.build())
                .username(meta.getUser())
                .password(meta.getPassword())
                .cacheMaxBytes(meta.getMaxbytes())
                .scanningFrequency(meta.getScanningFrequency())
                .connectTimeout(meta.getConnecttimeout())
                .version(meta.getStarRocksQueryVisitor().getStarRocksVersion())
                .expectDelayTime(expectDelayTime)
                .addHeader("timeout", String.valueOf(meta.getTimeout()))
                .addHeader("max_filter_ratio", String.valueOf(meta.getMaxFilterRatio()));

        return builder.build();

    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (StarRocksKettleConnectorMeta) smi;
        data = (StarRocksKettleConnectorData) sdi;

        try {
            if (data.streamLoadManager != null) {

                data.streamLoadManager.flush();
                data.streamLoadManager.close();

                if (data.streamLoadManager.getException() != null) {
                    logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailFlush"), data.streamLoadManager.getException());
                }

                data.streamLoadManager = null;
            }
        } catch (Exception e) {
            setErrors(1L);
            logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UNEXPECTEDERRORCLOSING"), e);
        }

        super.dispose(smi, sdi);
    }


}
