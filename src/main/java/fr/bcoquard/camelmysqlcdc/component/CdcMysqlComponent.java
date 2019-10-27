package fr.bcoquard.camelmysqlcdc.component;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component("cdc-mysql")
public class CdcMysqlComponent extends DefaultComponent {

    private boolean usePlaceholder = true;

    private String datasourceUrl;
    private String datasourcePort;
    private String datasourceSchema;
    private String datasourceUser;
    private String datasourcePassword;

    public CdcMysqlComponent() {
    }

    public CdcMysqlComponent(Class<? extends Endpoint> endpointClass) {
        super();
    }

    public CdcMysqlComponent(CamelContext context) {
        super(context);
    }

    public CdcMysqlComponent(CamelContext context, Class<? extends Endpoint> endpointClass) {
        super(context);
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("url", remaining.split(":")[0]);
        properties.put("port", remaining.split(":")[1]);
        properties.put("username", getAndRemoveParameter(parameters, "username", String.class, ""));
        properties.put("password",  getAndRemoveParameter(parameters, "password", String.class, ""));
        properties.put("tables",  getAndRemoveParameter(parameters, "tables", String.class, ""));

        CdcMysqlEndpoint endpoint = new CdcMysqlEndpoint(uri, this, properties);
        return endpoint;
    }
}
