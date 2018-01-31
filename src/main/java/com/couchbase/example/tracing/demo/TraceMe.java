package com.couchbase.example.tracing.demo;

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

        Tracer tracer = new Configuration(
                "simple_read_write",
                new Configuration.SamplerConfiguration("const", 1),
                new Configuration.ReporterConfiguration(
                        true, "localhost", 5775, 1000, 10000)
        ).getTracer();


        CouchbaseEnvironment env =
//                DefaultCouchbaseEnvironment.builder().tracer(tracer).build();
                DefaultCouchbaseEnvironment.builder().tracer(new SlowOperationTracer()).build();

                Cluster cluster = CouchbaseCluster.create(env);
        cluster.authenticate("ingenthr", "letmein");
        bucket = cluster.openBucket("default");
    }

    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name) {


        try {
            for (int i=1; i<10; i++ ) {
                System.out.println(bucket.get("u:king_arthur", 1, TimeUnit.MICROSECONDS));
            }
        } catch (Exception e) {
            // don't care
        }

        return new Greeting(counter.incrementAndGet(),
                String.format(template, name));
    }
}