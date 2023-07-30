package org.pentaho.di.ui.trans.steps.starrockskettleconnector;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.annotations.PluginDialog;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.starrockskettleconnector.StarRocksKettleConnectorMeta;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dialog class for the StarRocks Kettle Connector step.
 */
@PluginDialog(id = "StarRocksKettleConnector", image = "BLKMYSQL.svg", pluginType = PluginDialog.PluginType.STEP,
        documentationUrl = "")
public class StarRocksKettleConnectorDialog extends BaseStepDialog implements StepDialogInterface {

    private static Class<?> PKG = StarRocksKettleConnectorDialog.class;

    private CCombo wConnection;
    private StarRocksKettleConnectorMeta input;
    private ColumnInfo[] ciReturn;
    private Map<String, Integer> inputFields;
    private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

    public StarRocksKettleConnectorDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        input = (StarRocksKettleConnectorMeta) in;
        inputFields = new HashMap<String, Integer>();
    }

    @Override
    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
        props.setLook(shell);
        setShellImage(shell, input);
        return null;
    }
}
