package com.couchbase.example.tracing.demo;

import com.couchbase.client.core.tracing.ThresholdLogReporter;
import com.couchbase.client.core.tracing.ThresholdLogTracer;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlQuery;
import com.uber.jaeger.Configuration;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import rx.Observable;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class TraceMe {

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private Bucket bucket;
    CouchbaseEnvironment env;

    public TraceMe() {
//        Configuration config = new Configuration("couchbase", null, null);
//        config.setStatsFactory(...); // optional if you want to get metrics about tracer behavior


        env = DefaultCouchbaseEnvironment.builder().tracer(selectTracer("jaeger")).build();
                Cluster cluster = CouchbaseCluster.create(env);
        cluster.authenticate("ingenthr", "letmein");
        bucket = cluster.openBucket("travel-sample");


    }

    private Tracer selectTracer(String type) {
        Tracer tracer;
        if (type.equals("jaeger")) {
            tracer = new Configuration(
                    "simple_read_write",
                    new Configuration.SamplerConfiguration("const", 1),
                    new Configuration.ReporterConfiguration(
                            true, "localhost", 5775, 1000, 10000)
            ).getTracer();
            return tracer;
        } else if (type.equals("slowlog")) {
            tracer = ThresholdLogTracer.create(ThresholdLogReporter.builder().kvThreshold(100, TimeUnit.MILLISECONDS).build());
            return tracer;
        }
        throw new IllegalArgumentException("Invalid tracer requested.");
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


    @RequestMapping(method = RequestMethod.POST)
    public boolean login(@RequestParam(value="name") String name) {

        JsonDocument principal = bucket.get("user_" + name);
        principal.content().put("logintime", System.currentTimeMillis()/1000);
        if (bucket.upsert(principal) != null) {
            return true;
        }  else {
            return false;
        }

    }

    @RequestMapping("/itineraries")
    public List itineraries() {
        final Scope scope = env.tracer()
                .buildSpan("query-and-fetch")
                .startActive(true);

        bucket.async()
                .query(N1qlQuery.simple("select meta().id as id from `travel-sample` where type = \"route\" limit 1000")/* , scope.span() */)
                .flatMap(new Func1<AsyncN1qlQueryResult, Observable<AsyncN1qlQueryRow>>() {
                    public Observable<AsyncN1qlQueryRow> call(AsyncN1qlQueryResult result) {
                        return result.rows();
                    }
                }).flatMap(new Func1<AsyncN1qlQueryRow, Observable<JsonDocument>>() {
            public Observable<JsonDocument> call(AsyncN1qlQueryRow row) {
                return bucket.async().get(row.value().getString("id"), /* scope.span(), */ JsonDocument.class);
            }
        })
                .toBlocking()
                .last();

        scope.close();
        // n1ql query for what I want to see
        return new ArrayList();
    }

    @RequestMapping("/airportInfo")
    public Airport airportInfo() {

        return new Airport();
    }




}