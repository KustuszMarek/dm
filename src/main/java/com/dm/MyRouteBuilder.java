package com.dm;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.yaml.snakeyaml.Yaml;

public class MyRouteBuilder extends RouteBuilder {

    public void configure() {
        // TASK 1
        List<String> inputDirs = loadInputDirs();

        // TASK 2
        for (String dir : inputDirs) {
            from("file:" + dir + "?include=.*\\.csv&initialDelay=1000&delay=1000")
                .routeId("route-" + new java.util.Random().nextInt())
                .split(body().tokenize("\n")).streaming().filter(body().isNotEqualTo(""))
                .process(this::processLine)
                .to("freemarker:templates/message.ftl")
                .to("seda:aggregator");
        }

        from("seda:aggregator")
            .aggregate(constant(true), new GroupedBodyAggregationStrategy())
            .completionTimeout(5000)
            .to("validator:classpath:Format.xsd")
            .to("file:interview/src/data?fileName=output.xml");

        // TASK 3
        from("file:interview/src/data?include=.*\\.(xml|json)")
            .routeId("mainRoute")
            .choice()
                .when(header(Exchange.FILE_NAME).endsWith(".xml"))
                    .choice()
                        .when(xpath("/person/city = 'Karlsruhe'"))
                            .log("DE message from XML")
                            .to("file:interview/target/messages/de")
                        .when(xpath("/person/city = 'London'"))
                            .log("UK message from XML")
                            .transform(constant("foo"))
                            .to("file:interview/target/messages/uk?fileName=uk.txt&fileExist=Append")
                        .otherwise()
                            .log("Other XML message")
                            .to("file:interview/target/messages/others")
                    .endChoice()
                .when(header(Exchange.FILE_NAME).endsWith(".json"))
                    .to("direct:processJson")
            .end();

        // TASK 4
        from("direct:processJson")
            .routeId("jsonProcessor")
            .unmarshal().json(JsonLibrary.Jackson)
            .filter(jsonpath("$[?(@.city == 'London')]"))
                .log("UK message from JSON")
                .transform(constant("foo"))
                .to("file:interview/target/messages/uk?fileName=uk.txt&fileExist=Append");

        // TASK 5
        from("timer:restCall?period=10000")
            .routeId("restApiConsumer")
            .doTry()
                .to("https://jsonplaceholder.typicode.com/posts/1")
                .to("file:interview/target/messages?fileName=jsonplaceholder_response.json")
            .doCatch(Exception.class)
                .log("Error calling API: ${exception.message}")
            .end();

        from("timer:restCall?period=10000")
            .routeId("restApiConsumerError")
            .doTry()
            .to("https://jsonplaceholdererror.typicode.com/posts/1")
            .to("file:interview/target/messages?fileName=jsonplaceholder_response.json")
            .doCatch(Exception.class)
            .log("Error calling API: ${exception.message}")
            .end();
    }

    private void processLine(org.apache.camel.Exchange exchange) {
        String line = exchange.getIn().getBody(String.class);
        if (line == null) {
            return;
        }
        // Remove BOM if present
        if (line.startsWith("\uFEFF")) {
            line = line.substring(1);
        }
        line = line.trim();
        if (line.isEmpty()) {
            return;
        }

        String[] parts = line.split("[,;]");
        String code = parts.length > 0 ? parts[0].trim() : "UNKNOWN";
        String itemValue = parts.length > 1 ? parts[1].trim() : "";

        exchange.getIn().setHeader("code", code);
        exchange.getIn().setHeader("itemValue", itemValue);
    }

    public static class GroupedBodyAggregationStrategy implements org.apache.camel.AggregationStrategy {
        @Override
        public org.apache.camel.Exchange aggregate(org.apache.camel.Exchange oldExchange, org.apache.camel.Exchange newExchange) {
            String newBody = newExchange.getIn().getBody(String.class);
            // Extract only the <Item>...</Item> part and trim it
            String item = newBody.replace("<Items>", "").replace("</Items>", "").trim();

            if (oldExchange == null) {
                // First time: start with <Items> and the first item
                newExchange.getIn().setBody("<Items>\n  " + item);
                return newExchange;
            }

            String oldBody = oldExchange.getIn().getBody(String.class);
            // Append a newline, indentation, and the new item
            oldExchange.getIn().setBody(oldBody + "\n  " + item);
            return oldExchange;
        }

        @Override
        public void onCompletion(org.apache.camel.Exchange exchange) {
            if (exchange != null) {
                String body = exchange.getIn().getBody(String.class);
                // Close the <Items> tag
                if (body != null && !body.endsWith("\n</Items>")) {
                    exchange.getIn().setBody(body + "\n</Items>");
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> loadInputDirs() {
        Yaml yaml = new Yaml();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.yml")) {
            Map<String, Object> cfg = yaml.load(is);
            return (List<String>) cfg.get("inputDirs");
        } catch (Exception e) {
            throw new IllegalStateException("Cannot load inputDirs from application.yml", e);
        }
    }
}
