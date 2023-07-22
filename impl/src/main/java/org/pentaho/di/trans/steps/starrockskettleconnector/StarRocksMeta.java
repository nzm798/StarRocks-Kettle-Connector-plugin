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

    Long getMaxbytes();

    Long getMaxrows();

    Long getConnecttimeout();

    Long getTimeout();
}
