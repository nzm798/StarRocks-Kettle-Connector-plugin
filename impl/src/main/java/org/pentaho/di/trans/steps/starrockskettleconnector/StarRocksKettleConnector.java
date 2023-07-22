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
    public boolean init(StepMetaInterface smi,StepDataInterface sdi){
        meta=(StarRocksKettleConnectorMeta) smi;
        data=(StarRocksKettleConnectorData) sdi;


    }
}
