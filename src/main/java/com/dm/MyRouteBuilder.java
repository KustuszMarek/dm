package com.dm;

import org.apache.camel.builder.RouteBuilder;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * A Camel Java DSL Router
 */
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;

public class MyRouteBuilder extends RouteBuilder {

    public void configure() throws Exception {
        from("file:src/data?noop=true&include=.*\\.(xml|json)")
            .routeId("mainRoute")
            .choice()
                .when(header(Exchange.FILE_NAME).endsWith(".xml"))
                    .to("direct:processXml")
                .when(header(Exchange.FILE_NAME).endsWith(".json"))
                    .to("direct:processJson")
            .end();

        from("direct:processXml")
            .routeId("xmlProcessor")
            .choice()
                .when(xpath("/person/city = 'Karlsruhe'"))
                    .log("DE message from XML")
                    .to("file:target/messages/de")
                .when(xpath("/person/city = 'London'"))
                    .log("UK message from XML")
                    .transform(constant("foo"))
                    .to("file:target/messages/uk?fileName=uk.txt&fileExist=Append")
                .otherwise()
                    .log("Other XML message")
                    .to("file:target/messages/others");

        from("direct:processJson")
            .routeId("jsonProcessor")
            .unmarshal().json(JsonLibrary.Jackson)
            .filter(jsonpath("$[?(@.city == 'London')]"))
                .log("UK message from JSON")
                .transform(constant("foo"))
                .to("file:target/messages/uk?fileName=uk.txt&fileExist=Append");
    }
}
}
