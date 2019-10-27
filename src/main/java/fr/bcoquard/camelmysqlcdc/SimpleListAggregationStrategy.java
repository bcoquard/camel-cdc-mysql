package fr.bcoquard.camelmysqlcdc;

import org.apache.camel.Exchange;
import org.apache.camel.processor.aggregate.AggregationStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleListAggregationStrategy implements AggregationStrategy {
    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        if (oldExchange == null) {
            List list = new ArrayList();
            list.addAll((List) newExchange.getIn().getBody());
            newExchange.getIn().setHeader("AGGREGATED_MESSAGES", list);
        } else {
            List list = (ArrayList) oldExchange.getIn().getHeader("AGGREGATED_MESSAGES");
            list.addAll((List) newExchange.getIn().getBody());
            newExchange.getIn().setHeader("AGGREGATED_MESSAGES", list);
        }

        newExchange.getIn().setHeader("LAST_BINLOG_FILENAME", newExchange.getIn().getHeader("CDC_MYSQL_BINLOG_FILENAME"));
        newExchange.getIn().setHeader("LAST_BINLOG_POSITION", newExchange.getIn().getHeader("CDC_MYSQL_BINLOG_POSITION"));

        return newExchange;
    }

    private void collectNeedInfo(ConcurrentHashMap map, Exchange exchange) {
        System.out.println("Collected Information on while aggregating..");
    }
}