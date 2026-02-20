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
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class MyRouteBuilder extends RouteBuilder {

    public void configure() throws Exception {
        List<String> inputDirs = loadInputDirs();

        for (String dir : inputDirs) {
            from("file:" + dir + "?include=.*\\.csv&delete=true&initialDelay=1000&delay=1000")
                .routeId("route-" + new java.util.Random().nextInt())
                .split(body().tokenize("\n")).streaming().filter(body().isNotEqualTo(""))
                .process(exchange -> {
                    String line = exchange.getIn().getBody(String.class);
                    if (line == null) {
                        return;
                    }
                    // Remove BOM if present
                    if (line.startsWith("\uFEFF")) {
                        line = line.substring(1);
                    }
                    line = line.strip();
                    if (line.isEmpty()) {
                        return;
                    }

                    String[] parts = line.split("[,;]");
                    String code = parts.length > 0 ? parts[0].strip() : "UNKNOWN";
                    String itemValue = parts.length > 1 ? parts[1].strip() : "";

                    exchange.getIn().setHeader("code", code);
                    exchange.getIn().setHeader("itemValue", itemValue);
                })
                .to("freemarker:templates/message.ftl")
                .to("seda:aggregator");
        }

        from("seda:aggregator")
            .aggregate(constant(true), new GroupedBodyAggregationStrategy())
            .completionTimeout(2000) // Complete after 2 seconds of inactivity
            .to("validator:classpath:Format.xsd")
            .to("file:src/data?fileName=output.xml");
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
}
