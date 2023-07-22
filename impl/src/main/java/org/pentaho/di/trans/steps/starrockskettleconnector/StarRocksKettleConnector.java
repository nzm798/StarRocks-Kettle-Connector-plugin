package org.pentaho.di.trans.steps.starrockskettleconnector;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

public class StarRocksKettleConnector extends BaseDatabaseStep implements StepInterface {

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

                closeOutput();

                return false;
            }
        }
    }

    @Override
    public boolean init(StepMetaInterface smi,StepDataInterface sdi){
        meta=(StarRocksKettleConnectorMeta) smi;
        data=(StarRocksKettleConnectorData) sdi;

        if (super.init(smi,sdi)){
            // TODO:连接StarRocks数据库获得StreamLoadManagerV2
        }

    }

    @Override
    public void dispose(StepMetaInterface smi,StepDataInterface sdi){
        meta=(StarRocksKettleConnectorMeta) smi;
        data=(StarRocksKettleConnectorData) sdi;
        // TODO:释放连接资源
    }


    private void closeOutput() throws Exception{
        // TODO:关闭传输
    }

}
