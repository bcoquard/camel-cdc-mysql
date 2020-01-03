package fr.bcoquard.camelmysqlcdc.component;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.DefaultExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CdcMysqlConsumer extends DefaultConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(CdcMysqlConsumer.class);

    private Processor processor;
    private Map<String, String> properties;
    private BinaryLogClient binaryLogClient;
    private final long delay = 500L;
    private final Set<TableDefinition> allowedTableDefinition = new HashSet<>();
    private final Map<Long, TableDefinition> tableMap = new HashMap<>();

    private TreeMap<String, TreeMap<String, TreeMap<Integer, String>>> columnDefinition = null;

    public CdcMysqlConsumer(Endpoint endpoint, Processor processor) {
        super(endpoint, processor);
    }

    protected MySQLConnection mySQLConnection;

    public CdcMysqlConsumer(Endpoint endpoint, Map<String, String> properties, Processor processor) {
        super(endpoint, processor);
        this.properties = properties;
        this.processor = processor;
    }

    @Override
    public CdcMysqlEndpoint getEndpoint() {
        return (CdcMysqlEndpoint) super.getEndpoint();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        String url = properties.get("url");
        String port = properties.get("port");
        String username = properties.get("username");
        String password = properties.get("password");
        String schema = properties.get("schema");

        this.mySQLConnection = new MySQLConnection(url, Integer.parseInt(port), username, password);


        if (this.binaryLogClient == null) {
            if (!properties.containsKey("schema")) {
                this.binaryLogClient = new BinaryLogClient(properties.get("url"), Integer.parseInt(properties.get("port")), properties.get("username"), properties.get("password"));
            } else {
                this.binaryLogClient = new BinaryLogClient(properties.get("url"), Integer.parseInt(properties.get("port")), properties.get("schema"), properties.get("username"), properties.get("password"));
            }

            if (properties.containsKey("tables")) {
                for (String iter : properties.get("tables").split(",")) {
                    String database = iter.split("\\.")[0];
                    String table = iter.split("\\.")[1];

                    this.allowedTableDefinition.add(new TableDefinition(database, table));
                }
            }

            EventDeserializer eventDeserializer = new EventDeserializer();
            binaryLogClient.setEventDeserializer(eventDeserializer);

            binaryLogClient.registerEventListener(event -> {
                try {
                    Exchange exchange = new DefaultExchange(getEndpoint().createExchange());

                    // Set commonly used headers
                    exchange.getIn().setHeader("CDC_MYSQL_BINLOG_FILENAME", binaryLogClient.getBinlogFilename());
                    exchange.getIn().setHeader("CDC_MYSQL_BINLOG_POSITION", binaryLogClient.getBinlogPosition());

                    doProcess(exchange, event);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        columnDefinition = mySQLConnection.queryTablesDefinition();

        binaryLogClient.connect();
    }

    private void doProcess(Exchange exchange, Event event) throws Exception {
        EventData data = event.getData();

        if (data instanceof TableMapEventData) {
            TableMapEventData tableData = (TableMapEventData) data;
            tableMap.put(tableData.getTableId(), new TableDefinition(tableData.getDatabase(), tableData.getTable()));
        } else if (data instanceof WriteRowsEventData) {
            WriteRowsEventData eventData = (WriteRowsEventData) data;
            TableDefinition currentDefinition = tableMap.get(eventData.getTableId());
            if (allowedTableDefinition.contains(currentDefinition)) {
                exchange.getIn().setHeader("CDC_MYSQL_EVENT_TYPE", "INSERT");
                setCommonHeader(exchange, currentDefinition);
                mapRows(exchange, currentDefinition, eventData.getRows(), EMessageOperationType.INSERT);
            } else {
                LOG.debug("Event not in tracked definition, skipping it");
            }
        } else if (data instanceof UpdateRowsEventData) {
            UpdateRowsEventData eventData = (UpdateRowsEventData) data;
            TableDefinition currentDefinition = tableMap.get(eventData.getTableId());
            if (allowedTableDefinition.contains(currentDefinition)) {
                exchange.getIn().setHeader("CDC_MYSQL_EVENT_TYPE", "UPDATE");
                setCommonHeader(exchange, currentDefinition);
                mapRows(exchange, currentDefinition, eventData.getRows(), EMessageOperationType.UPDATE);
            } else {
                LOG.debug("Event not in tracked definition, skipping it");
            }
        } else if (data instanceof DeleteRowsEventData) {
            DeleteRowsEventData eventData = (DeleteRowsEventData) data;
            TableDefinition currentDefinition = tableMap.get(eventData.getTableId());
            if (allowedTableDefinition.contains(currentDefinition)) {
                exchange.getIn().setHeader("CDC_MYSQL_EVENT_TYPE", "DELETE");
                setCommonHeader(exchange, currentDefinition);
                mapRows(exchange, currentDefinition, eventData.getRows(), EMessageOperationType.DELETE);
            } else {
                LOG.debug("Event not in tracked definition, skipping it");
            }
        }

        // Call next processor
        if (exchange.getIn().getBody() != null) {
            processor.process(exchange);
        }
    }

    private void setCommonHeader(Exchange exchange, TableDefinition tableDefinition) {
        exchange.getIn().setHeader("CDC_MYSQL_EVENT_DATABASE_NAME", tableDefinition.database);
        exchange.getIn().setHeader("CDC_MYSQL_EVENT_TABLE_NAME", tableDefinition.table);
    }

    private <T> void mapRows(Exchange exchange, TableDefinition tableDefinition, List<T> rows, EMessageOperationType eMessageOperationType) throws Exception {
        List<DMLMessage> messages = new ArrayList<>();
        for (Object row : rows) {
            DMLMessage dmlMessage = new DMLMessage(eMessageOperationType);

            MessagePayload messagePayloadOld = null;
            MessagePayload messagePayloadNew = null;

            if (row instanceof Map.Entry) {
                messagePayloadOld = formatResult(tableDefinition.database, tableDefinition.table, (Object[]) ((Map.Entry) row).getValue());
                messagePayloadNew = formatResult(tableDefinition.database, tableDefinition.table, (Object[]) ((Map.Entry) row).getValue());
            } else if (row instanceof Object[]) {
                if (eMessageOperationType.equals(EMessageOperationType.INSERT)) {
                    messagePayloadNew = formatResult(tableDefinition.database, tableDefinition.table, (Object[]) row);
                } else if (eMessageOperationType.equals(EMessageOperationType.DELETE)) {
                    messagePayloadOld = formatResult(tableDefinition.database, tableDefinition.table, (Object[]) row);
                } else {
                    throw new Exception();
                }
            } else {
                throw new Exception();
            }

            dmlMessage.addPayload(PayloadType.OLD, messagePayloadOld);
            dmlMessage.addPayload(PayloadType.NEW, messagePayloadNew);

            messages.add(dmlMessage);
        }

        exchange.getIn().setBody(messages);
    }

    @Override
    protected void doStop() throws Exception {

        if (this.binaryLogClient != null && this.binaryLogClient.isConnected()) {
            this.binaryLogClient.disconnect();
            while (this.binaryLogClient.isConnected()) {
                // Wait until disconnected
            }
            LOG.info(this.binaryLogClient.getBinlogFilename() + "-" + this.binaryLogClient.getBinlogPosition());
        }
        LOG.info("Cdc Mysql Disconnected");
        super.doStop();
    }

    @Override
    protected void doShutdown() throws Exception {

        if (this.binaryLogClient != null && this.binaryLogClient.isConnected()) {
            this.binaryLogClient.disconnect();
            while (this.binaryLogClient.isConnected()) {
                // Wait until disconnected
            }
            LOG.info(this.binaryLogClient.getBinlogFilename() + "-" + this.binaryLogClient.getBinlogPosition());
        }

        LOG.info("Cdc Mysql got shutdown");
        super.doShutdown();
    }

    private MessagePayload formatResult(String schema, String tableName, Object[] row) {
        MessagePayload messagePayload = new MessagePayload();

        for (int i = 0; i < row.length; i++) {
            String column = columnDefinition.get(schema).get(tableName).get(i + 1);
            messagePayload.addOne(column, row[i]);
        }

        return messagePayload;
    }

    private class TableDefinition {
        private String database;
        private String table;

        public TableDefinition() {
        }

        public TableDefinition(String database, String table) {
            this.database = database;
            this.table = table;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableDefinition that = (TableDefinition) o;
            return Objects.equals(database, that.database) &&
                    Objects.equals(table, that.table);
        }

        @Override
        public int hashCode() {
            return Objects.hash(database, table);
        }
    }
}
