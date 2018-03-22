package com.sam.enrichment.processor;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class ConsoleCustomProcessor implements CustomProcessorRuntime {
    protected static final Logger LOG = LoggerFactory.getLogger(ConsoleCustomProcessor.class);
    public static final String CONFIG_FIELD_NAME = "configField";
    Map<String, Object> config = new HashMap<>();

    public ConsoleCustomProcessor()
    {

    }


    public void initialize(Map<String, Object> config) {
        if (config != null) {
            this.config = config;
        }
        LOG.info("Initializing with config field " + CONFIG_FIELD_NAME + " = " + this.config.get(CONFIG_FIELD_NAME));
    }


    public void validateConfig(Map<String, Object> config) {
        LOG.debug("Validating config ");
        if (!config.containsKey(CONFIG_FIELD_NAME)) {
           LOG.error("Config Not Valid");
        }
        LOG.debug("Config valid ");
    }


    public List<StreamlineEvent> process(StreamlineEvent event) throws ProcessingException {
        LOG.info("Processing {}", event);
        return Arrays.asList(event);
    }


    public void cleanup() {
        LOG.debug("Cleaning up");
    }
}