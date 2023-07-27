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
            try {
                data.streamLoadManager=new StreamLoadManagerV2(getProperties(meta),true);
                data.streamLoadManager.init();
            }catch (Exception e){
                logError(BaseMessages.getString(PKG,"StarRocksKettleConnector.Message.FailConnManager"),e);
                return false;
            }
            data.tablename = meta.getTablename();
            // TODO:需要根据是否更新和删除导入data中的colums。
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
        }
    }

    // Get the property values needed for Stream Load loading.
    public StreamLoadProperties getProperties(StarRocksKettleConnectorMeta meta){
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
                .enableUpsertDelete(true);
        // TODO:添加部分列导入功能

        StreamLoadProperties.Builder builder=StreamLoadProperties.builder()
                .loadUrls(meta.getLoadurl().toArray(new String[0]))
                .jdbcUrl(meta.getJdbcurl())
                .defaultTableProperties(defaultTablePropertiesBuilder.build())
                .username(meta.getUser())
                .password(meta.getPassword())
                .cacheMaxBytes(meta.getMaxbytes())
                .connectTimeout(meta.getConnecttimeout())
                .version(meta.getStarRocksQueryVisitor().getStarRocksVersion());

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
