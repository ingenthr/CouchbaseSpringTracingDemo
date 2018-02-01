package com.couchbase.example.tracing.demo;

import com.couchbase.client.core.tracing.SlowOperationReporter;
import com.couchbase.client.core.tracing.SlowOperationTracer;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.uber.jaeger.Configuration;
import io.opentracing.Tracer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class TraceMe {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private Bucket bucket;

    public TraceMe() {
//        Configuration config = new Configuration("couchbase", null, null);
//        config.setStatsFactory(...); // optional if you want to get metrics about tracer behavior


        CouchbaseEnvironment env =
//                DefaultCouchbaseEnvironment.builder().tracer(tracer).build();
                DefaultCouchbaseEnvironment.builder().tracer(selectTracer("jaeger")).build();
                Cluster cluster = CouchbaseCluster.create(env);
        cluster.authenticate("ingenthr", "letmein");
        bucket = cluster.openBucket("default");
    }

    private Tracer selectTracer(String type) {


        if (type.equals("jaeger")) {
            Tracer tracer = new Configuration(
                    "simple_read_write",
                    new Configuration.SamplerConfiguration("const", 1),
                    new Configuration.ReporterConfiguration(
                            true, "localhost", 5775, 1000, 10000)
            ).getTracer();
            return tracer;
        } else if (type.equals("slowlog")) {
            return new SlowOperationTracer(SlowOperationReporter.builder().kvThreshold(100000).build());
        }

        throw new IllegalArgumentException("Invalid tracer requested");

    }

    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {


        try {
            for (int i=0; i<1000; i++) {
                bucket.get("u:king_arthur", 500, TimeUnit.MILLISECONDS);
            }
        } catch (RuntimeException e) {
            System.err.println("Hit exception " + e.getMessage() + " of class " + e.getClass());
        }

        return new Greeting(counter.incrementAndGet(),
                String.format(template, name));
    }
}