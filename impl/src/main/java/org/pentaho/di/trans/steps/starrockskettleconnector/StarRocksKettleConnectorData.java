package org.pentaho.di.trans.steps.starrockskettleconnector;

import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class StarRocksKettleConnectorData extends BaseStepData implements StepDataInterface {

    // Use the Stream Load method to load the number.
    private StreamLoadManagerV2 streamLoadManager;
}
