import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.iceberg.*;
import org.apache.iceberg.camel.IcebergCatalog;
import org.apache.iceberg.camel.IcebergComponent;
import org.apache.iceberg.catalog.*;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

/** Two copies of the SAME message (same ICEBERG_DEDUP_KEY) processed at the same time, keyed producer.
 *  Usage: ConcurrentKey <trials> */
public class ConcurrentKey {
    public static void main(String[] a) throws Exception {
        int trials = Integer.parseInt(a[0]), dupTrials = 0;
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        File wh = Files.createTempDirectory("ck").toFile();
        HadoopCatalog admin = new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), wh.getAbsolutePath());
        IcebergCatalog cat = new SourceRedelivery.NonNullIcebergCatalog();
        cat.setName("t"); cat.setType("hadoop"); cat.setWarehouseLocation(wh.getAbsolutePath());
        DefaultCamelContext ctx = new DefaultCamelContext();
        IcebergComponent comp = new IcebergComponent();
        Field f = IcebergComponent.class.getDeclaredField("catalog"); f.setAccessible(true); f.set(comp, cat);
        ctx.addComponent("iceberg", comp);
        // two consumers = two producers (two routes), like two nodes or two JMS consumers
        ctx.addRoutes(new RouteBuilder() { public void configure() {
            from("direct:c1").to("iceberg:sink"); from("direct:c2").to("iceberg:sink"); }});
        ctx.start();
        ProducerTemplate pt = ctx.createProducerTemplate();
        ExecutorService ex = Executors.newFixedThreadPool(2);
        for (int t = 0; t < trials; t++) {
            TableIdentifier id = TableIdentifier.of(Namespace.of("db"), "t" + t);
            admin.createTable(id, schema, PartitionSpec.unpartitioned());
            CyclicBarrier go = new CyclicBarrier(2);
            List<Future<?>> fs = new ArrayList<>();
            for (String ep : List.of("direct:c1", "direct:c2")) {
                fs.add(ex.submit(() -> { go.await();
                    pt.send(ep, e -> { e.getMessage().setHeader("ICEBERG_TABLE", "db.t" + id.name().substring(1));
                        e.getMessage().setHeader("ICEBERG_DEDUP_KEY", "orders-001.csv@1700000000000");
                        Map<String, Object> r = new HashMap<>(); r.put("id", 1); r.put("name", "a");
                        e.getMessage().setBody(List.of(r)); });
                    return null; }));
            }
            for (Future<?> x : fs) x.get();
            int n = 0;
            try (CloseableIterable<Record> it = IcebergGenerics.read(admin.loadTable(id)).build()) { for (Record r : it) n++; }
            if (n > 1) dupTrials++;
        }
        ex.shutdown(); ctx.stop();
        System.out.printf("  %-4s keyed producer, 2 concurrent copies of one message: duplicate rows in %d of %d trials%n",
                dupTrials > 0 ? "BUG" : "ok", dupTrials, trials);
    }
}
