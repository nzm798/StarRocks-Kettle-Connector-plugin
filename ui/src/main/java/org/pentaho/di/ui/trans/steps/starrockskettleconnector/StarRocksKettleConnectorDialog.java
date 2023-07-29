package org.pentaho.di.ui.trans.steps.starrockskettleconnector;

import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.widgets.Display;
import org.pentaho.di.core.annotations.PluginDialog;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Dialog class for the StarRocks Kettle Connector step.
 */
@PluginDialog( id = "StarRocksKettleConnector", image = "BLKMYSQL.svg", pluginType = PluginDialog.PluginType.STEP,
        documentationUrl = "" )
public class StarRocksKettleConnectorDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG= StarRocksKettleConnectorDialog.class;

    private CCombo wConnection;
    @Override
    public String open() {
        Shell parent=getParent();
        Display display=parent.getDisplay();
        return null;
    }
}
