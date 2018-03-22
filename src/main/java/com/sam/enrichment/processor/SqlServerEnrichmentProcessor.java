package com.sam.enrichment.processor;

import com.google.common.base.Strings;
import com.hortonworks.streamline.streams.Result;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class SqlServerEnrichmentProcessor implements CustomProcessorRuntime {
    protected static final Logger LOG = LoggerFactory
            .getLogger(SqlServerEnrichmentProcessor.class);

    public static final String CONFIG_ENRICHMENT_SQL = "enrichmentSQL";
    public static final String CONFIG_ENRICHED_OUTPUT_FIELDS = "enrichedOutputFields";
    public static final String CONFIG_DB_CONNECTION_URL = "dbConnectionURL";
    public static final String CONFIG_DB_CLASSNAME = "dbClassName";
    public static final String CONFIG_DB_USERNAME = "dbUserName";
    public static final String CONFIG_DB_PASSWORD = "dbPassword";

    
    private static HikariConfig hikariConfig = new HikariConfig();
    private static HikariDataSource ds;


    private Connection sqlServerConnection = null;
    private String enrichmentSQLStatement = null;
    private String[] enrichedOutPutFields;


    public SqlServerEnrichmentProcessor()
    {

    }



    public void cleanup() {
        DbUtils.closeQuietly(sqlServerConnection);

    }



    /**
     * Initializing the JDBC connection to Sql Server
     */
    public void initialize(Map<String, Object> config) {
        LOG.info("Initializing + " + SqlServerEnrichmentProcessor.class.getName());


        this.enrichmentSQLStatement =  ((String) config.get(CONFIG_ENRICHMENT_SQL)).trim();
        LOG.info("The configured enrichment SQL is: " + enrichmentSQLStatement);

        String outputFields = (String) config
                .get(CONFIG_ENRICHED_OUTPUT_FIELDS);
        String outputFieldsCleaned = StringUtils.deleteWhitespace(outputFields);
        this.enrichedOutPutFields = outputFieldsCleaned.split(",");
        LOG.info("Enriched Output fields is: " + enrichedOutPutFields);

        setUpJDBCSqlServerConnection(config);

    }


    /**
     * Enrich the event with teh results from the user provided sql queries
     */
    public List<StreamlineEvent> process(StreamlineEvent event)
            throws ProcessingException {
        LOG.info("Event[" + event + "] about to be enriched");

        StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
        builder.putAll(event);

        /* Enrich */
        Map<String, Object> enrichValues = enrich(event);
        LOG.info("Enriching events[" + event
                + "]  with the following enriched values: " + enrichValues);
        builder.putAll(enrichValues);

        /* Build the enriched streamline event and return */
        List<Result> results = new ArrayList<Result>();
        StreamlineEvent enrichedEvent = builder.dataSourceId(
                event.getDataSourceId()).build();
        LOG.info("Enriched StreamLine Event is: " + enrichedEvent);

        List<StreamlineEvent> newEvents = Collections
                .<StreamlineEvent> singletonList(enrichedEvent);

        return newEvents;
    }

    /**
     * Returns a map of the the results of the sql queries where the key is the
     * schema input field name and the value is the enriched value
     *
     * @param event
     * @return
     */
    private Map<String, Object> enrich(StreamlineEvent event) {


        Map<String, Object> enrichedValues = new HashMap<String, Object>();

        StrSubstitutor strSub = new StrSubstitutor(event);

        String enrichSQLToExecute = strSub.replace(this.enrichmentSQLStatement);
        ResultSet rst = null;
        Statement statement = null;
        try {

            LOG.info("The SQL with substitued fields to be executed is: "
                    + enrichSQLToExecute);

            statement = sqlServerConnection.createStatement();
            rst = statement.executeQuery(enrichSQLToExecute);

            if (rst.next()) {
                int columnCount = rst.getMetaData().getColumnCount();
                for (int i = 1, count=0; i <= columnCount; i++) {
                    enrichedValues.put(enrichedOutPutFields[count++],
                            Strings.nullToEmpty(rst.getString(i)));
                }
            } else {
                String errorMsg = "No results found for enrichment query: "
                        + enrichSQLToExecute;
                LOG.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        } catch (SQLException e) {
            String errorMsg = "Error enriching event[" + event
                    + "] with enrichment sql[" + this.enrichmentSQLStatement + "]";
            LOG.error(errorMsg, e);
            throw new RuntimeException(errorMsg, e);

        } finally {
            DbUtils.closeQuietly(rst);
            DbUtils.closeQuietly(statement);

        }

        return enrichedValues;
    }

   

    private void setUpJDBCSqlServerConnection(Map<String, Object> config) {
    	String jdbcConnectionUrl = (String) config.get(CONFIG_DB_CONNECTION_URL);
    	  LOG.info("Initializing Sql Connection with JDBC connection string["
                  + jdbcConnectionUrl + "]");
        try {
        	hikariConfig.setJdbcUrl(jdbcConnectionUrl);
        	
        	hikariConfig.setUsername((String) config.get(CONFIG_DB_USERNAME));
        	hikariConfig.setPassword((String) config.get(CONFIG_DB_PASSWORD));
        	hikariConfig.addDataSourceProperty( "cachePrepStmts" , "true" );
        	hikariConfig.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        	hikariConfig.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        	hikariConfig.addDataSourceProperty("maximumPoolSize", 100);
            ds = new HikariDataSource(hikariConfig);
        	sqlServerConnection = ds.getConnection();
        	
        } catch (SQLException e) {
            String error = "Error creating Sql Server JDBC connection";
            LOG.error(error, e);
            throw new RuntimeException(error);
        }
        LOG.info("Successfully created Sql Server with JDBC connection string["
                + jdbcConnectionUrl + "]");
    }




	@Override
	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		// TODO Auto-generated method stub
		
	}
}