package fr.bcoquard.camelmysqlcdc;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class CamelMysqlCdcApplication extends RouteBuilder {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(CamelMysqlCdcApplication.class, args);
    }

    @Override
    public void configure() throws Exception {
        from("cdc-mysql:localhost:3306?username=root&password=password&tables=test.toto")
                .log("${header.CDC_MYSQL_BINLOG_FILENAME} - ${header.CDC_MYSQL_BINLOG_POSITION} - " +
                        "${header.CDC_MYSQL_EVENT_TYPE} - ${header.CDC_MYSQL_EVENT_DATABASE_NAME} - ${header.CDC_MYSQL_EVENT_TABLE_NAME}")
                .aggregate().constant(true).aggregationStrategy(new SimpleListAggregationStrategy())
                .completionSize(100)
                .completionTimeout(100)
                .log("${header.AGGREGATED_MESSAGES}")
                .log("${header.LAST_BINLOG_FILENAME} - ${header.LAST_BINLOG_POSITION}");
    }
}
