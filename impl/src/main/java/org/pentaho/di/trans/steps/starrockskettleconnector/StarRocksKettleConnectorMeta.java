package org.pentaho.di.trans.steps.starrockskettleconnector;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

@Step( id = "StarRocksKettleConnector", name = "BaseStep.TypeLongDesc.StarRocksKettleConnector",
        description = "BaseStep.TypeTooltipDesc.StarRocksKettleConnector",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Bulk",
        image = "BLKMYSQL.svg",
        documentationUrl = "http://wiki.pentaho.com/display/EAI/StarRocks+Kettle+Connector",
        i18nPackageName = "org.pentaho.di.trans.steps.starrockskettleconnector" )
@InjectionSupported( localizationPrefix = "StarRocksKettleConnector.Injection.", groups = { "FIELDS" } )
public class StarRocksKettleConnectorMeta extends BaseStepMeta implements StarRocksMeta {

    private static Class<?> PKG=StarRocksKettleConnectorMeta.class; // for i18n purposes, needed by Translator2!!

    private static final long KILO_BYTES_SCALE = 1024L;
    private static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final long GIGA_BYTES_SCALE = MEGA_BYTES_SCALE * KILO_BYTES_SCALE;

    /** Url of the stream load, if you you don't specify the http/https prefix, the default http. like: `fe_ip1:http_port;http://fe_ip2:http_port;https://fe_nlb`. */
    @Injection(name = "LOAD_URL")
    private List<String> loadurl;

    /** Url of the jdbc like: `jdbc:mysql://fe_ip1:query_port,fe_ip2:query_port...`. */
    @Injection(name = "JDBC_URL")
    private String jdbcurl;

    /** Database name of the stream load. */
    @Injection(name = "DATABASE_NAME")
    private String databasename;

    /** Table name of the stream load. */
    @Injection(name = "TABLE_NAME")
    private String tablename;

    /** StarRocks user name. */
    @Injection(name = "USER")
    private String user;

    /** StarRocks user password. */
    @Injection(name = "PASSWORD")
    private String password;

    /** The format of the data to be loaded. The value can be CSV or JSON. */
    @Injection(name = "FORMAT")
    private String format;

    /** The maximum size of data that can be loaded into StarRocks at a time. */
    @Injection(name = "MAXBYTES")
    private Long maxbytes;

    /** The maximum number of rows that can be loaded into StarRocks at one time. */
    @Injection(name = "MAXROWS")
    private Long maxrows;

    /** Timeout period for connecting to the load-url. */
    @Injection(name = "CONNECT_TIMEOUT")
    private Long connecttimeout;

    /** Stream Load timeout period, in seconds. */
    @Injection(name = "TIMEOUT")
    private Long timeout;

    /** Field name of the target table */
    @Injection( name = "FIELD_TABLE", group = "FIELDS" )
    private String[] fieldTable;

    /** Field name in the stream */
    @Injection( name = "FIELD_STREAM", group = "FIELDS" )
    private String[] fieldStream;

    /**
     * @param loadurl Url of the stream load.
     */
    public void setLoadurl(List<String> loadurl){
        this.loadurl=loadurl;
    }

    /**
     *
     * @return Return the load url.
     */
    public List<String> getLoadurl(){
        return this.loadurl;
    }

    /**
     *
     * @return Return the JDBC url.
     */
    public String getJdbcurl() {
        return jdbcurl;
    }

    /**
     * @param jdbcurl Url of the jdbc.
     */
    public void setJdbcurl(String jdbcurl){
        this.jdbcurl=jdbcurl;
    }

    /**
     *
     * @return Return the data base name.
     */
    public String getDatabasename() {
        return databasename;
    }

    /**
     *
     * @param databasename The target data base name.
     */
    public void setDatabasename(String databasename) {
        this.databasename = databasename;
    }

    /**
     *
     * @return Return the table name.
     */
    public String getTablename() {
        return tablename;
    }

    /**
     *
     * @param tablename The target table name.
     */
    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    /**
     *
     * @return Return the StarRocks user.
     */
    public String getUser() {
        return user;
    }

    /**
     *
     * @param user The StarRocks user.
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     *
     * @return Returns the password for the user.
     */
    public String getPassword() {
        return password;
    }

    /**
     *
     * @param password The password for the user.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     *
     * @return Returns the imported data format.
     */
    public String getFormat() {
        return format;
    }

    /**
     *
     * @param format The imported data format
     */
    public void setFormat(String format) {
        this.format = format;
    }

    /**
     *
     * @return Return the maximum size of data that can be loaded into StarRocks at a time.
     */
    public Long getMaxbytes() {
        return maxbytes;
    }

    /**
     *
     * @param maxbytes The maximum size of data that can be loaded into StarRocks at a time.
     */
    public void setMaxbytes(Long maxbytes) {
        this.maxbytes = maxbytes;
    }

    /**
     *
     * @return Return the maximum number of rows that can be loaded into StarRocks at one time.
     */
    public Long getMaxrows() {
        return maxrows;
    }

    /**
     *
     * @param maxrows The maximum number of rows that can be loaded into StarRocks at one time.
     */
    public void setMaxrows(Long maxrows) {
        this.maxrows = maxrows;
    }

    /**
     *
     * @return Return timeout period for connecting to the load-url.
     */
    public Long getConnecttimeout() {
        return connecttimeout;
    }

    /**
     *
     * @param connecttimeout Timeout period for connecting to the load-url.
     */
    public void setConnecttimeout(Long connecttimeout) {
        this.connecttimeout = connecttimeout;
    }

    /**
     *
     * @return Return Stream Load timeout period.
     */
    public Long getTimeout() {
        return timeout;
    }

    /**
     *
     * @param timeout Stream Load timeout period.
     */
    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Returns the fieldTable.
     */
    public String[] getFieldTable() {
        return fieldTable;
    }

    /**
     * @param fieldTable
     *          The fieldTable to set.
     */
    public void setFieldTable( String[] fieldTable ) {
        this.fieldTable = fieldTable;
    }

    /**
     * @return Returns the fieldStream.
     */
    public String[] getFieldStream() {
        return fieldStream;
    }

    /**
     * @param fieldStream
     *          The fieldStream to set.
     */
    public void setFieldStream( String[] fieldStream ) {
        this.fieldStream = fieldStream;
    }

    public void setDefault(){
        loadurl=null;
        jdbcurl=null;
        databasename="";
        tablename= BaseMessages.getString(PKG,"StarRocksKettleConnectorMeta.DefaultTableName");
        user="root";
        password="";
        format="CSV";
        maxbytes=90L * MEGA_BYTES_SCALE;
        maxrows=500000L;
        connecttimeout=1000L;
        timeout=600L;
    }

    public void allocate( int nrvalues ){
        fieldTable=new String[nrvalues];
        fieldStream=new String[nrvalues];
    }

    public Object clone() {
        StarRocksKettleConnectorMeta retval=(StarRocksKettleConnectorMeta) super.clone();
        int nrvalues= fieldTable.length;
        retval.allocate(nrvalues);
        System.arraycopy(fieldTable,0,retval.fieldTable,0,nrvalues);
        System.arraycopy(fieldTable,0,retval.fieldTable,0,nrvalues);

        return retval;
    }

    public void loadXML(Node stepnode, IMetaStore metaStore) throws KettleXMLException{
        readData(stepnode);
    }
    private void readData(Node stepnode)throws KettleXMLException{
        try {
            String loadurl1= XMLHandler.getTagValue(stepnode,"loadurl");
            loadurl= Arrays.asList(loadurl1.split(";"));
            jdbcurl=XMLHandler.getTagValue(stepnode,"jdbcurl");
            databasename=XMLHandler.getTagValue(stepnode,"databasename");
            tablename=XMLHandler.getTagValue(stepnode,"tablename");
            user=XMLHandler.getTagValue(stepnode,"user");
            password=XMLHandler.getTagValue(stepnode,"password");
            format=XMLHandler.getTagValue(stepnode,"format");
            maxbytes=Long.valueOf(XMLHandler.getTagValue(stepnode,"maxbytes"));
            maxrows=Long.valueOf(XMLHandler.getTagValue(stepnode,"maxrows"));
            connecttimeout=Long.valueOf(XMLHandler.getTagValue(stepnode,"connecttimeout"));
            timeout=Long.valueOf(XMLHandler.getTagValue(stepnode,"timeout"));

            // Field data mapping
            int nrvalues=XMLHandler.countNodes(stepnode,"mapping");
            allocate(nrvalues);

            for (int i=0;i<nrvalues;i++){
                Node vnode=XMLHandler.getSubNodeByNr(stepnode,"mapping",i);

                fieldTable[i]=XMLHandler.getTagValue(vnode,"stream_name");
                fieldStream[i]=XMLHandler.getTagValue(vnode,"field_name");
                if (fieldStream[i]==null){
                    fieldStream[i] = fieldTable[i]; // default: the same name!
                }
            }

        }catch (Exception e){
            throw new KettleXMLException(BaseMessages.getString(PKG,"StarRocksKettleConnectorMeta.Exception.UnableToReadStepInfoFromXML"),e);
        }

    }
}
