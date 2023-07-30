package org.pentaho.di.ui.trans.steps.starrockskettleconnector;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.PluginDialog;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.steps.starrockskettleconnector.StarRocksKettleConnectorMeta;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TextVar;
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

    private Label wlLoadUrl;
    private TextVar wLoadUrl;
    private FormData fdlLoadUrl,fdLoadUrl;

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

        ModifyListener lsMod = new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent modifyEvent) {
                input.setChanged();
            }
        };

        FocusListener lsFocusLost = new FocusAdapter() {
            @Override
            public void focusLost(FocusEvent focusEvent) {
                setTableFieldCombo();
            }
        };
        changed = input.hasChanged();

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;
        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "StarRocksKettleConnectorDialog.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = Const.MARGIN;

        // Stepname line
        wlStepname = new Label(shell, SWT.RIGHT);
        wlStepname.setText(BaseMessages.getString(PKG,"StarRocksKettleConnectorDialog.Stepname.Label"));
        props.setLook(wlStepname);
        fdlStepname=new FormData();
        fdlStepname.left=new FormAttachment(0,0);
        fdlStepname.right=new FormAttachment(middle,-margin);
        fdlStepname.top=new FormAttachment(0,margin);
        wlStepname.setLayoutData( fdlStepname );
        wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
        wStepname.setText( stepname );
        props.setLook( wStepname );
        wStepname.addModifyListener( lsMod );
        fdStepname = new FormData();
        fdStepname.left = new FormAttachment( middle, 0 );
        fdStepname.top = new FormAttachment( 0, margin );
        fdStepname.right = new FormAttachment( 100, 0 );
        wStepname.setLayoutData( fdStepname );

        // Load Url line...
        wlLoadUrl=new Label(shell,SWT.RIGHT);
        wlLoadUrl.setText(BaseMessages.getString(PKG,"StarRocksKettleConnectorDialog.LoadUrl.Label"));
        props.setLook(wlLoadUrl);
        fdlLoadUrl=new FormData();
        fdlLoadUrl.left=new FormAttachment(0,0);
        fdlLoadUrl.right=new FormAttachment(middle,-margin);
        fdlLoadUrl.top=new FormAttachment(wStepname,margin*2);
        wlLoadUrl.setLayoutData(fdlLoadUrl);

        wLoadUrl=new TextVar(transMeta,shell,SWT.SINGLE|SWT.LEFT|SWT.BORDER);
        props.setLook(wLoadUrl);
        wLoadUrl.addModifyListener(lsMod);
        wLoadUrl.addFocusListener(lsFocusLost);
        fdLoadUrl=new FormData();
        fdLoadUrl.left=new FormAttachment(middle,0);
        fdLoadUrl.right=new FormAttachment(100,0);
        fdLoadUrl.top=new FormAttachment(wStepname,margin*2);
        wLoadUrl.setLayoutData(fdLoadUrl);

        // JDBC Url ...

        return null;
    }

    private void setTableFieldCombo() {
        // TODO:关联的组件失去焦点时被调用.
    }
}
