package org.pentaho.di.trans.steps.starrockskettleconnector;

import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import com.trilead.ssh2.packets.PacketUserauthBanner;
import org.pentaho.di.core.util.StreamLogger;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.starrockskettleconnector.starrocks.StarRocksISerializer;

/**
 * Stores data for the StarRocks Kettle Connector step.
 */
public class StarRocksKettleConnectorData extends BaseStepData implements StepDataInterface {

    // Use the Stream Load method to load the number.
    public StreamLoadManagerV2 streamLoadManager;

    public StarRocksISerializer serializer;
    // In StarRocks,If you want to implement changes to the data and partial imports, you need to add '__op'.
    public String[] columns;
    //The index corresponding to the data type of the row element.
    public int[] keynrs; // nr of keylookup -value in row...


    public String tablename;



    public StarRocksKettleConnectorData(){
        super();

        streamLoadManager=null;
    }
}
