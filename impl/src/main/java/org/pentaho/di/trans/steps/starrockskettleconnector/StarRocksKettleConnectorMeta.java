package org.pentaho.di.trans.steps.starrockskettleconnector;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.util.List;

@Step( id = "StarRocksKettleConnector", name = "BaseStep.TypeLongDesc.StarRocksKettleConnector",
        description = "BaseStep.TypeTooltipDesc.StarRocksKettleConnector",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Bulk",
        image = "BLKMYSQL.svg",
        documentationUrl = "http://wiki.pentaho.com/display/EAI/StarRocks+Kettle+Connector",
        i18nPackageName = "org.pentaho.di.trans.steps.starrockskettleconnector" )
@InjectionSupported( localizationPrefix = "StarRocksKettleConnector.Injection.", groups = { "FIELDS" } )
public class StarRocksKettleConnectorMeta extends BaseStepMeta implements StepMetaInterface {

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
}
