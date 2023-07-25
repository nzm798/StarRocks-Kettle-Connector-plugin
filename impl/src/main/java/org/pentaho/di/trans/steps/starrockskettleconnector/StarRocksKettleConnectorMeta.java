package org.pentaho.di.trans.steps.starrockskettleconnector;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.lang.String;
import java.util.Arrays;
import java.util.List;

@Step(id = "StarRocksKettleConnector", name = "BaseStep.TypeLongDesc.StarRocksKettleConnector",
        description = "BaseStep.TypeTooltipDesc.StarRocksKettleConnector",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Bulk",
        image = "BLKMYSQL.svg",
        documentationUrl = "http://wiki.pentaho.com/display/EAI/StarRocks+Kettle+Connector",
        i18nPackageName = "org.pentaho.di.trans.steps.starrockskettleconnector")
@InjectionSupported(localizationPrefix = "StarRocksKettleConnector.Injection.", groups = {"FIELDS"})
public class StarRocksKettleConnectorMeta extends BaseStepMeta implements StarRocksMeta {

    private static Class<?> PKG = StarRocksKettleConnectorMeta.class; // for i18n purposes, needed by Translator2!!

    private static final long KILO_BYTES_SCALE = 1024L;
    private static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final long GIGA_BYTES_SCALE = MEGA_BYTES_SCALE * KILO_BYTES_SCALE;

    /**
     * Url of the stream load, if you you don't specify the http/https prefix, the default http. like: `fe_ip1:http_port;http://fe_ip2:http_port;https://fe_nlb`.
     */
    @Injection(name = "LOAD_URL")
    private List<String> loadurl;

    /**
     * Url of the jdbc like: `jdbc:mysql://fe_ip1:query_port,fe_ip2:query_port...`.
     */
    @Injection(name = "JDBC_URL")
    private String jdbcurl;

    /**
     * Database name of the stream load.
     */
    @Injection(name = "DATABASE_NAME")
    private String databasename;

    /**
     * Table name of the stream load.
     */
    @Injection(name = "TABLE_NAME")
    private String tablename;

    /**
     * StarRocks user name.
     */
    @Injection(name = "USER")
    private String user;

    /**
     * StarRocks user password.
     */
    @Injection(name = "PASSWORD")
    private String password;

    /**
     * The format of the data to be loaded. The value can be CSV or JSON.
     */
    @Injection(name = "FORMAT")
    private String format;

    /**
     * The maximum size of data that can be loaded into StarRocks at a time.
     */
    @Injection(name = "MAXBYTES")
    private long maxbytes;

    /**
     * The maximum number of rows that can be loaded into StarRocks at one time.
     */
    @Injection(name = "MAXROWS")
    private long maxrows;

    /**
     * Timeout period for connecting to the load-url.
     */
    @Injection(name = "CONNECT_TIMEOUT")
    private long connecttimeout;

    /**
     * Stream Load timeout period, in seconds.
     */
    @Injection(name = "TIMEOUT")
    private long timeout;

    /**
     * Field name of the target table
     */
    @Injection(name = "FIELD_TABLE", group = "FIELDS")
    private String[] fieldTable;

    /**
     * Field name in the stream
     */
    @Injection(name = "FIELD_STREAM", group = "FIELDS")
    private String[] fieldStream;

    /**
     * @param loadurl Url of the stream load.
     */
    public void setLoadurl(List<String> loadurl) {
        this.loadurl = loadurl;
    }

    /**
     * @return Return the load url.
     */
    public List<String> getLoadurl() {
        return this.loadurl;
    }

    /**
     * @return Return the JDBC url.
     */
    public String getJdbcurl() {
        return jdbcurl;
    }

    /**
     * @param jdbcurl Url of the jdbc.
     */
    public void setJdbcurl(String jdbcurl) {
        this.jdbcurl = jdbcurl;
    }

    /**
     * @return Return the data base name.
     */
    public String getDatabasename() {
        return databasename;
    }

    /**
     * @param databasename The target data base name.
     */
    public void setDatabasename(String databasename) {
        this.databasename = databasename;
    }

    /**
     * @return Return the table name.
     */
    public String getTablename() {
        return tablename;
    }

    /**
     * @param tablename The target table name.
     */
    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    /**
     * @return Return the StarRocks user.
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user The StarRocks user.
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return Returns the password for the user.
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password The password for the user.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return Returns the imported data format.
     */
    public String getFormat() {
        return format;
    }

    /**
     * @param format The imported data format
     */
    public void setFormat(String format) {
        this.format = format;
    }

    /**
     * @return Return the maximum size of data that can be loaded into StarRocks at a time.
     */
    public long getMaxbytes() {
        return maxbytes;
    }

    /**
     * @param maxbytes The maximum size of data that can be loaded into StarRocks at a time.
     */
    public void setMaxbytes(long maxbytes) {
        this.maxbytes = maxbytes;
    }

    /**
     * @return Return the maximum number of rows that can be loaded into StarRocks at one time.
     */
    public long getMaxrows() {
        return maxrows;
    }

    /**
     * @param maxrows The maximum number of rows that can be loaded into StarRocks at one time.
     */
    public void setMaxrows(long maxrows) {
        this.maxrows = maxrows;
    }

    /**
     * @return Return timeout period for connecting to the load-url.
     */
    public long getConnecttimeout() {
        return connecttimeout;
    }

    /**
     * @param connecttimeout Timeout period for connecting to the load-url.
     */
    public void setConnecttimeout(long connecttimeout) {
        this.connecttimeout = connecttimeout;
    }

    /**
     * @return Return Stream Load timeout period.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * @param timeout Stream Load timeout period.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Returns the fieldTable.
     */
    public String[] getFieldTable() {
        return fieldTable;
    }

    /**
     * @param fieldTable The fieldTable to set.
     */
    public void setFieldTable(String[] fieldTable) {
        this.fieldTable = fieldTable;
    }

    /**
     * @return Returns the fieldStream.
     */
    public String[] getFieldStream() {
        return fieldStream;
    }

    /**
     * @param fieldStream The fieldStream to set.
     */
    public void setFieldStream(String[] fieldStream) {
        this.fieldStream = fieldStream;
    }

    public void setDefault() {
        fieldTable=null;
        loadurl = null;
        jdbcurl = null;
        databasename = "";
        tablename = BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.DefaultTableName");
        user = "root";
        password = "";
        format = "CSV";
        maxbytes = 90L * MEGA_BYTES_SCALE;
        maxrows = 500000L;
        connecttimeout = 1000L;
        timeout = 600L;
    }

    public void allocate(int nrvalues) {
        fieldTable = new String[nrvalues];
        fieldStream = new String[nrvalues];
    }

    public Object clone() {
        StarRocksKettleConnectorMeta retval = (StarRocksKettleConnectorMeta) super.clone();
        int nrvalues = fieldTable.length;
        retval.allocate(nrvalues);
        System.arraycopy(fieldTable, 0, retval.fieldTable, 0, nrvalues);
        System.arraycopy(fieldTable, 0, retval.fieldTable, 0, nrvalues);

        return retval;
    }

    public void loadXML(Node stepnode, IMetaStore metaStore) throws KettleXMLException {
        readData(stepnode);
    }

    private void readData(Node stepnode) throws KettleXMLException {
        try {
            String loadurl1 = XMLHandler.getTagValue(stepnode, "loadurl");
            loadurl = Arrays.asList(loadurl1.split(";"));
            jdbcurl = XMLHandler.getTagValue(stepnode, "jdbcurl");
            databasename = XMLHandler.getTagValue(stepnode, "databasename");
            tablename = XMLHandler.getTagValue(stepnode, "tablename");
            user = XMLHandler.getTagValue(stepnode, "user");
            password = XMLHandler.getTagValue(stepnode, "password");
            format = XMLHandler.getTagValue(stepnode, "format");
            maxbytes = Long.valueOf(XMLHandler.getTagValue(stepnode, "maxbytes"));
            maxrows = Long.valueOf(XMLHandler.getTagValue(stepnode, "maxrows"));
            connecttimeout = Long.valueOf(XMLHandler.getTagValue(stepnode, "connecttimeout"));
            timeout = Long.valueOf(XMLHandler.getTagValue(stepnode, "timeout"));

            // Field data mapping
            int nrvalues = XMLHandler.countNodes(stepnode, "mapping");
            allocate(nrvalues);

            for (int i = 0; i < nrvalues; i++) {
                Node vnode = XMLHandler.getSubNodeByNr(stepnode, "mapping", i);

                fieldTable[i] = XMLHandler.getTagValue(vnode, "stream_name");
                fieldStream[i] = XMLHandler.getTagValue(vnode, "field_name");
                if (fieldStream[i] == null) {
                    fieldStream[i] = fieldTable[i]; // default: the same name!
                }
            }

        } catch (Exception e) {
            throw new KettleXMLException(BaseMessages.getString(PKG, "StarRocksKettleConnectorMeta.Exception.UnableToReadStepInfoFromXML"), e);
        }

    }

    public String getXML() {
        StringBuilder retval = new StringBuilder(300);

        String loadurl1= String.join(";",loadurl);
        retval.append( "    " ).append( XMLHandler.addTagValue( "loadurl", loadurl1 ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "jdbcurl", jdbcurl ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "databasename", databasename ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "tablename", tablename ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "user", user ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "password", password ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "format", format ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "maxbytes", maxbytes ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "maxrows", maxrows ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "connecttimeout", connecttimeout ) );
        retval.append( "    " ).append( XMLHandler.addTagValue( "timeout", timeout ) );

        for ( int i = 0; i < fieldTable.length; i++ ) {
            retval.append( "        " ).append( XMLHandler.addTagValue( "stream_name", fieldTable[i] ) );
            retval.append( "        " ).append( XMLHandler.addTagValue( "field_name", fieldStream[i] ) );
            retval.append( "      </mapping>" ).append( Const.CR );
        }

        return retval.toString();
    }

    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step) throws KettleException{
        try {
            String loadurl1=rep.getStepAttributeString(id_step,"loadurl");
            loadurl=Arrays.asList(loadurl1.split(";"));
            jdbcurl=rep.getStepAttributeString(id_step,"jdbcurl");
            databasename=rep.getStepAttributeString(id_step,"databasename");
            tablename=rep.getStepAttributeString(id_step,"tablename");
            user=rep.getStepAttributeString(id_step,"user");
            password=rep.getStepAttributeString(id_step,"password");
            format=rep.getStepAttributeString(id_step,"format");
            maxbytes=Long.valueOf(rep.getStepAttributeString(id_step,"maxbytes"));
            maxrows=Long.valueOf(rep.getStepAttributeString(id_step,"maxrows"));
            connecttimeout=Long.valueOf(rep.getStepAttributeString(id_step,"connecttimeout"));
            timeout=Long.valueOf(rep.getStepAttributeString(id_step,"timeout"));

            int nrvalues = rep.countNrStepAttributes( id_step, "stream_name" );

            allocate( nrvalues );

            for ( int i = 0; i < nrvalues; i++ ) {
                fieldTable[i] = rep.getStepAttributeString( id_step, i, "stream_name" );
                fieldStream[i] = rep.getStepAttributeString( id_step, i, "field_name" );
                if ( fieldStream[i] == null ) {
                    fieldStream[i] = fieldTable[i];
                }
            }
        }catch (Exception e){
            throw new KettleException(BaseMessages.getString(PKG,"StarRocksKettleConnectorMeta.Exception.UnexpectedErrorReadingStepInfoFromRepository"),e);
        }
    }

    public void saveRep(Repository rep,IMetaStore metaStore,ObjectId id_transformation,ObjectId id_step)throws KettleException{
        try {
            String loadurl1=String.join(";",loadurl);
            rep.saveStepAttribute(id_transformation,id_step,"loadurl",loadurl1);
            rep.saveStepAttribute(id_transformation,id_step,"jdbcurl",jdbcurl);
            rep.saveStepAttribute(id_transformation,id_step,"databasename",databasename);
            rep.saveStepAttribute(id_transformation,id_step,"tablename",tablename);
            rep.saveStepAttribute(id_transformation,id_step,"user",user);
            rep.saveStepAttribute(id_transformation,id_step,"password",password);
            rep.saveStepAttribute(id_transformation,id_step,"format",format);
            rep.saveStepAttribute(id_transformation,id_step,"maxbytes",maxbytes);
            rep.saveStepAttribute(id_transformation,id_step,"maxrows",maxrows);
            rep.saveStepAttribute(id_transformation,id_step,"connecttimeout",connecttimeout);
            rep.saveStepAttribute(id_transformation,id_step,"timeout",timeout);

            for ( int i = 0; i < fieldTable.length; i++ ) {
                rep.saveStepAttribute( id_transformation, id_step, i, "stream_name", fieldTable[i] );
                rep.saveStepAttribute( id_transformation, id_step, i, "field_name", fieldStream[i] );
            }
        }catch (Exception e){
            throw new KettleException(BaseMessages.getString(PKG,"StarRocksKettleConnectorMeta.Exception.UnableToSaveStepInfoToRepository")+id_step,e);
        }
    }

    public void getFields(RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
        // Default: nothing changes to rowMeta
    }

    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
                      IMetaStore metaStore ) {
        // TODO:每个Step都有机会验证其设置并验证用户给出的配置是否合理。
        CheckResult cr;
        String error_message = "";


    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
                                 Trans trans ) {
        return new StarRocksKettleConnector( stepMeta, stepDataInterface, cnr, transMeta, trans );
    }

    public StepDataInterface getStepData() {
        return new StarRocksKettleConnectorData();
    }
}
