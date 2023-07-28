package org.pentaho.di.trans.steps.starrockskettleconnector;

import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.List;

public interface StarRocksMeta extends StepMetaInterface {
    List<String> getLoadurl();

    String getJdbcurl();

    String getDatabasename();

    String getTablename();

    String getUser();

    String getPassword();

    String getFormat();

    long getMaxbytes();

    long getMaxrows();

    int getConnecttimeout();

    int getTimeout();
    boolean getPartialUpdate();
    String[] getPartialcolumns();

    boolean getEnableUpsertDelete();
    String getUpsertOrDelete();
}
