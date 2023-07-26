package org.pentaho.di.trans.steps.starrockskettleconnector;

import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.pentaho.di.core.util.StreamLogger;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Stores data for the StarRocks Kettle Connector step.
 */
public class StarRocksKettleConnectorData extends BaseStepData implements StepDataInterface {

    // Use the Stream Load method to load the number.
    private StreamLoadManagerV2 streamLoadManager;

    public int[] keynrs; // nr of keylookup -value in row...

    public StreamLogger errorLogger;
    public StreamLogger outputLogger;

    public String tablename;



    public StarRocksKettleConnectorData(){
        super();

        streamLoadManager=null;
    }
}
