package fr.bcoquard.camelmysqlcdc.component;

import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultPollingEndpoint;

import java.util.Map;

public class CdcMysqlEndpoint extends DefaultPollingEndpoint {

    Map<String, String> properties;

    public CdcMysqlEndpoint() {
    }

    public CdcMysqlEndpoint(String uri, Component component, Map<String, String> properties) {
        super(uri, component);
        this.properties = properties;
    }

    @Override
    public Producer createProducer() throws Exception {
        return null;
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        CdcMysqlConsumer cdcMysqlConsumer = new CdcMysqlConsumer(this, properties, processor);
        return cdcMysqlConsumer;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }
}
