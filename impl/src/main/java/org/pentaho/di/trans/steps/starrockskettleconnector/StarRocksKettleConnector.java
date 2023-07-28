package org.pentaho.di.trans.steps.starrockskettleconnector;

import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksCsvSerializer;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksISerializer;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksJsonSerializer;

import java.util.Arrays;
import java.util.stream.Collectors;

public class StarRocksKettleConnector extends BaseStep implements StepInterface {

    private static Class<?> PKG= StarRocksKettleConnectorMeta.class;
    private StarRocksKettleConnectorMeta meta;
    private StarRocksKettleConnectorData data;

    public StarRocksKettleConnector(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans){
        super(stepMeta,stepDataInterface,copyNr,transMeta,trans);
    }

    @Override
    public boolean processRow(StepMetaInterface smi,StepDataInterface sdi){
        meta=(StarRocksKettleConnectorMeta) smi;
        data=(StarRocksKettleConnectorData) sdi;

        try {
            Object[] r=getRow(); // Get row from input rowset & set row busy!
            if ( r == null ) { // no more input to be expected...

                setOutputDone();

                return false;
            }
            if (first){
                first=false;

                // Cache field indexes.
                data.keynrs=new int[meta.getFieldStream().length];
                for (int i=0;i<data.keynrs.length;i++){
                    data.keynrs[i]=getInputRowMeta().indexOfValue(meta.getFieldStream()[i]);
                }
            }
        }catch (Exception e){
            logError(BaseMessages.getString(PKG,"StarRocksKettleConnector.Log.ErrorInStep"));
            setErrors(1);
            stopAll();
            setOutputDone();
            return false;
        }
    }

    @Override
    public boolean init(StepMetaInterface smi,StepDataInterface sdi){
        meta=(StarRocksKettleConnectorMeta) smi;
        data=(StarRocksKettleConnectorData) sdi;

        if (super.init(smi,sdi)){
            // Add columns properties to all to prevent changes in the order of the fields.
            if (meta.getPartialUpdate()){
                data.columns=new String[meta.getPartialcolumns().length];
                System.arraycopy(meta.getPartialcolumns(),0,data.columns,0,meta.getPartialcolumns().length);
            }else {
                data.columns=new String[meta.getFieldTable().length];
                System.arraycopy(meta.getFieldTable(),0,data.columns,0,meta.getFieldTable().length);
            }
            try {
                data.streamLoadManager=new StreamLoadManagerV2(getProperties(meta,data),true);
                data.streamLoadManager.init();
            }catch (Exception e){
                logError(BaseMessages.getString(PKG,"StarRocksKettleConnector.Message.FailConnManager"),e);
                return false;
            }
            data.tablename = meta.getTablename();
            return true;
        }
        return false;

    }

    public StarRocksISerializer getSerializer(StarRocksKettleConnectorMeta meta){
        StarRocksISerializer serializer;
        if (meta.getFormat().equals("CSV")){
            serializer=new StarRocksCsvSerializer(",");
        }else if (meta.getFormat().equals("JSON")){
            serializer=new StarRocksJsonSerializer(meta.getFieldTable());
        }else {
            logError(BaseMessages.getString(PKG,"StarRocksKettleConnector.Message.FailFormat"));
            return null;
        }
        return serializer;
    }

    // Get the property values needed for Stream Load loading.
    public StreamLoadProperties getProperties(StarRocksKettleConnectorMeta meta,StarRocksKettleConnectorData data){
        StreamLoadDataFormat dataFormat;
        if (meta.getFormat().equals("CSV")){
            dataFormat=StreamLoadDataFormat.CSV;
        } else if (meta.getFormat().equals("JSON")) {
            dataFormat=StreamLoadDataFormat.JSON;
        }else {
            throw new RuntimeException("data format are not support");
        }
        StreamLoadTableProperties.Builder defaultTablePropertiesBuilder=StreamLoadTableProperties.builder()
                .database(meta.getDatabasename())
                .table(meta.getTablename())
                .streamLoadDataFormat(dataFormat)
                .enableUpsertDelete(meta.getEnableUpsertDelete());
        if (meta.getPartialUpdate()){
            defaultTablePropertiesBuilder.addProperty("partial_update","true");
        }
        // Add the '__op' field
        if (data.columns!=null){
            // don't need to add "columns" header in following cases
            // 1. use csv format but the flink and starrocks schemas are aligned
            // 2. use json format, except that it's loading o a primary key table for StarRocks 1.x
            boolean noNeedAddColumnsHeader;
            if (dataFormat instanceof StreamLoadDataFormat.CSVFormat){
                noNeedAddColumnsHeader=false;
            }else {
                noNeedAddColumnsHeader=!meta.getEnableUpsertDelete() || meta.isOpAutoProjectionInJson();
            }
            if (!noNeedAddColumnsHeader){
                String[] headerColumns;
                if (meta.getEnableUpsertDelete()){
                    headerColumns=new String[data.columns.length+1];
                    System.arraycopy(data.columns,0,headerColumns,0,data.columns.length);
                    headerColumns[data.columns.length]="__op";
                }else {
                    headerColumns= data.columns;
                }
                String cols = Arrays.stream(headerColumns)
                        .map(f -> String.format("`%s`", f.trim().replace("`", "")))
                        .collect(Collectors.joining(","));
                defaultTablePropertiesBuilder.columns(cols);
            }
        }

        StreamLoadProperties.Builder builder=StreamLoadProperties.builder()
                .loadUrls(meta.getLoadurl().toArray(new String[0]))
                .jdbcUrl(meta.getJdbcurl())
                .defaultTableProperties(defaultTablePropertiesBuilder.build())
                .username(meta.getUser())
                .password(meta.getPassword())
                .cacheMaxBytes(meta.getMaxbytes())
                .connectTimeout(meta.getConnecttimeout())
                .version(meta.getStarRocksQueryVisitor().getStarRocksVersion())
                .addHeader("timeout",String.valueOf(meta.getTimeout()));

        return builder.build();

    }

    @Override
    public void dispose(StepMetaInterface smi,StepDataInterface sdi){
        meta=(StarRocksKettleConnectorMeta) smi;
        data=(StarRocksKettleConnectorData) sdi;

        try {
            if (data.streamLoadManager!=null) {
                try {
                    data.streamLoadManager.flush();
                } catch (Exception e) {
                    logError(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailFlush"), e);
                }
                data.streamLoadManager.close();
                data.streamLoadManager=null;
            }
        }catch (Exception e){
            setErrors(1L);
            logError(BaseMessages.getString(PKG,"StarRocksKettleConnector.Message.UNEXPECTEDERRORCLOSING"),e);
        }

        super.dispose(smi,sdi);
    }


}
