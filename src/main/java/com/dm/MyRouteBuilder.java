package com.dm;

import org.apache.camel.builder.RouteBuilder;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * A Camel Java DSL Router
 */
import org.apache.camel.builder.RouteBuilder;

public class MyRouteBuilder extends RouteBuilder {

    public void configure() throws Exception {
        from("file:src/data?noop=true&include=.*\\.xml")
            .choice()
                .when(xpath("/person/city = 'Karlsruhe'"))
                    .log("DE message")
                    .to("file:target/messages/de")
                .when(xpath("/person/city = 'London'"))
                    .log("UK message")
                    .transform(constant("foo"))
                    .to("file:target/messages/uk?fileName=uk.txt")
                .otherwise()
                    .log("Other message")
                    .to("file:target/messages/others");
    }
}
}
