package com.tmwsystems.streaming.tests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.sam.enrichment.processor.SqlServerEnrichmentCacheableProcessor;
import com.sam.enrichment.processor.SqlServerEnrichmentProcessor;

public class SqlServerEnrichmentCacheableProcessorTests {
	
	private static final Object HR_ENRICHMENT_SQL = "SELECT BASEURL," + 
			"TOKEN," + 
			"FreightHaulerProviderXID," + 
			"CarrierMC," + 
			"CarrierDOT," + 
			"CarrierSCAC," + 
			"TMSType," + 
			"BOLRefType," + 
			"CheckCallType" + 
			" FROM vw_CarrierDataFeedConfig WHERE Tenant_Conf_Tenant_Id = ${tenantId} AND ExtractDatasource_Id = ${datasourceId} ";
	private static final Object HR_OUTPUT_FIELDS = "BASEURL,TOKEN,FreightHaulerProviderXID,CarrierMC,CarrierDOT,CarrierSCAC,TMSType,BOLRefType,CheckCallType";
	protected static final Logger LOG = LoggerFactory.getLogger(SqlServerEnrichmentCacheableProcessorTests.class);
	private static final Object DB_CONNECTION_URL = "jdbc:sqlserver://tmwbi.database.windows.net;database=meta_cdc_extract;user=cdcetlusr@tmwbi;password=ChangeData(2017);encrypt=false;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
	private static final Object DB_USERNAME = "cdcetlusr@tmwbi";
	private static final Object DB_PASSWORD = "ChangeData(2017)";


	
	@Test
	public void testHREnrichmentNonSecureCluster() throws Exception {
		SqlServerEnrichmentCacheableProcessor enrichProcessor = new SqlServerEnrichmentCacheableProcessor();
		Map<String, Object> processorConfig = createHREnrichmentConfig();
		enrichProcessor.validateConfig(processorConfig);
		enrichProcessor.initialize(processorConfig);
		
		List<StreamlineEvent> eventResults = enrichProcessor.process(createStreamLineEvent());
		
		
		List<StreamlineEvent> eventResultsFromCache = enrichProcessor.process(createStreamLineEvent1());

		LOG.info("Result of enrichment is: " + ReflectionToStringBuilder.toString(eventResults));
		
		LOG.info("Result of enrichment is: " + ReflectionToStringBuilder.toString(eventResultsFromCache));

	}
	
	
	private StreamlineEvent createStreamLineEvent() {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		keyValues.put("tenantId", 615);
		keyValues.put("datasourceId", 137);
		keyValues.put("lat", 1234);
		keyValues.put("lon", 2345);
		
		StreamlineEvent event = StreamlineEventImpl.builder().build().addFieldsAndValues(keyValues);
		
		System.out.println("Input StreamLIne event is: " + ReflectionToStringBuilder.toString(event));

		
		return event;
	}
	
	private StreamlineEvent createStreamLineEvent1() {
		Map<String, Object> keyValues = new HashMap<String, Object>();
		keyValues.put("tenantId", 615);
		keyValues.put("datasourceId", 137);
		keyValues.put("lat", 1234);
		
		StreamlineEvent event = StreamlineEventImpl.builder().build().addFieldsAndValues(keyValues);
		
		System.out.println("Input StreamLIne event is: " + ReflectionToStringBuilder.toString(event));

		
		return event;
	}

	private Map<String, Object> createHREnrichmentConfig() {
		Map<String, Object> processorConfig = new HashMap<String, Object>();
		
		processorConfig.put(SqlServerEnrichmentProcessor.CONFIG_ENRICHMENT_SQL, HR_ENRICHMENT_SQL);
		processorConfig.put(SqlServerEnrichmentProcessor.CONFIG_ENRICHED_OUTPUT_FIELDS, HR_OUTPUT_FIELDS);
		processorConfig.put(SqlServerEnrichmentProcessor.CONFIG_DB_CONNECTION_URL, DB_CONNECTION_URL);
		processorConfig.put(SqlServerEnrichmentProcessor.CONFIG_DB_USERNAME, DB_USERNAME);
		processorConfig.put(SqlServerEnrichmentProcessor.CONFIG_DB_PASSWORD, DB_PASSWORD);

		
				
		return processorConfig;
	}

}
