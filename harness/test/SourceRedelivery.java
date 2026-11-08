import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.camel.IcebergCatalog;
import org.apache.iceberg.camel.IcebergComponent;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

/**
 * Source redelivery after a successful Iceberg commit, with a real Camel file consumer.
 *   from("file:in").convert rows.to("iceberg:sink").process(next step)
 * Scenarios:
 *   later  : the next step fails once; the file consumer leaves the file and polls it again (new exchange)
 *   crash1 : the JVM halts right after the Iceberg step (before the file is moved); run crash2 next
 *   crash2 : a restarted JVM polls the same file again
 * Usage: SourceRedelivery <variant> <scenario> <workdir>
 */
public class SourceRedelivery {

    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    /** Same workaround for C2 as in Harness (drop null catalog properties). */
    public static class NonNullIcebergCatalog extends IcebergCatalog {
        @Override
        public Map<String, String> properties() {
            Map<String, String> m = new HashMap<>(super.properties());
            m.values().removeIf(v -> v == null);
            return m;
        }
    }

    public static void main(String[] args) throws Exception {
        String variant = args[0];
        String scenario = args[1];
        File work = new File(args[2]);
        File warehouse = new File(work, "warehouse");
        File in = new File(work, "in");
        HadoopCatalog admin = new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), warehouse.getAbsolutePath());
        TableIdentifier id = TableIdentifier.of(Namespace.of("db"), "orders");

        if (!scenario.equals("crash2")) {
            in.mkdirs();
            admin.createTable(id, SCHEMA, PartitionSpec.unpartitioned());
            Files.writeString(new File(in, "orders-001.csv").toPath(), "1,a\n2,b\n");
        }

        IcebergCatalog cat = new NonNullIcebergCatalog();
        cat.setName("test");
        cat.setType("hadoop");
        cat.setWarehouseLocation(warehouse.getAbsolutePath());

        DefaultCamelContext ctx = new DefaultCamelContext();
        IcebergComponent comp = new IcebergComponent();
        Field f = IcebergComponent.class.getDeclaredField("catalog");
        f.setAccessible(true);
        f.set(comp, cat);
        ctx.addComponent("iceberg", comp);
        AtomicInteger nextStepCalls = new AtomicInteger();
        List<String> exchangeIds = new ArrayList<>();
        ctx.addRoutes(new RouteBuilder() {
            public void configure() {
                from("file:" + in.getAbsolutePath() + "?initialDelay=0&delay=200")
                        .process(e -> {
                            exchangeIds.add(e.getExchangeId());
                            List<Map<String, Object>> rows = new ArrayList<>();
                            for (String line : e.getMessage().getBody(String.class).split("\n")) {
                                String[] p = line.split(",");
                                Map<String, Object> r = new HashMap<>();
                                r.put("id", Integer.parseInt(p[0]));
                                r.put("name", p[1]);
                                rows.add(r);
                            }
                            e.getMessage().setBody(rows);
                            e.getMessage().setHeader("ICEBERG_TABLE", "db.orders");
                            e.getMessage().setHeader("ICEBERG_DEDUP_KEY",
                                    e.getMessage().getHeader("CamelFileName") + "@"
                                            + e.getMessage().getHeader("CamelFileLastModified"));
                        })
                        .to("iceberg:sink")
                        .process(e -> {
                            int n = nextStepCalls.incrementAndGet();
                            if (scenario.equals("crash1")) {
                                System.out.println("  committed to Iceberg; halting the JVM before the file is acknowledged");
                                System.out.flush();
                                Runtime.getRuntime().halt(0);
                            }
                            if (scenario.equals("later") && n == 1) {
                                throw new IllegalStateException("next step unavailable (once)");
                            }
                        });
            }
        });
        ctx.start();
        long deadline = System.currentTimeMillis() + 15000;
        while (System.currentTimeMillis() < deadline && new File(in, "orders-001.csv").exists()) {
            Thread.sleep(100);
        }
        Thread.sleep(500);
        ctx.stop();

        Table t = admin.loadTable(id);
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> it = IcebergGenerics.read(t).build()) {
            for (Record r : it) {
                rows.add(r.getField("id") + ":" + r.getField("name"));
            }
        }
        rows.sort(null);
        int snapshots = 0;
        for (Object s : t.snapshots()) {
            snapshots++;
        }
        boolean consumed = !new File(in, "orders-001.csv").exists();
        boolean dup = rows.size() > 2;
        System.out.printf("  %-4s variant=%s scenario=%s: file consumed=%s, deliveries in this JVM=%d (exchange ids %s), "
                + "snapshots=%d, rows=%s%n", dup ? "BUG" : "ok", variant, scenario, consumed, exchangeIds.size(), exchangeIds,
                snapshots, rows);
    }
}
