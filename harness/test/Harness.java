import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.camel.IcebergCatalog;
import org.apache.iceberg.camel.IcebergComponent;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;

/**
 * Real-code checks for jbonofre/iceberg@CAMEL camel-iceberg (234bcff).
 * Usage: Harness <variant>  where variant is orig | close | fixed (which IcebergProducer is first on the classpath).
 */
public class Harness {

    /** A HadoopCatalog that counts instances and closes, and can lose the reply of one commit. */
    public static class TestCatalog extends HadoopCatalog {
        static final AtomicInteger CREATED = new AtomicInteger();
        static final AtomicInteger CLOSED = new AtomicInteger();
        static final AtomicBoolean LOSE_NEXT_REPLY = new AtomicBoolean();

        public TestCatalog() {
            CREATED.incrementAndGet();
        }

        @Override
        public void close() throws java.io.IOException {
            CLOSED.incrementAndGet();
            super.close();
        }

        @Override
        protected TableOperations newTableOps(TableIdentifier id) {
            TableOperations d = super.newTableOps(id);
            return new TableOperations() {
                public TableMetadata current() { return d.current(); }
                public TableMetadata refresh() { return d.refresh(); }
                public void commit(TableMetadata base, TableMetadata metadata) {
                    d.commit(base, metadata); // the commit is applied ...
                    if (LOSE_NEXT_REPLY.getAndSet(false)) { // ... but the client never hears back
                        throw new CommitStateUnknownException(new RuntimeException("connection lost after commit"));
                    }
                }
                public FileIO io() { return d.io(); }
                public EncryptionManager encryption() { return d.encryption(); }
                public String metadataFileLocation(String f) { return d.metadataFileLocation(f); }
                public LocationProvider locationProvider() { return d.locationProvider(); }
            };
        }
    }

    /** Workaround for C2 in the test setup only: drop the null entries properties() adds. */
    public static class NonNullIcebergCatalog extends IcebergCatalog {
        @Override
        public Map<String, String> properties() {
            Map<String, String> m = new HashMap<>(super.properties());
            m.values().removeIf(v -> v == null);
            return m;
        }
    }

    static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    static String variant;
    static File warehouse;
    static int fails = 0;

    static void check(String what, boolean bugShown, String detail) {
        System.out.printf("  %-9s %s -- %s%n", bugShown ? "BUG" : "ok", what, detail);
    }

    static IcebergCatalog icebergCatalog(boolean testImpl) {
        IcebergCatalog c = new NonNullIcebergCatalog();
        c.setName("test");
        if (testImpl) {
            c.setImplementation(TestCatalog.class.getName());
        } else {
            c.setType("hadoop");
        }
        c.setWarehouseLocation(warehouse.getAbsolutePath());
        return c;
    }

    static Table createTable(String name, boolean partitioned) {
        HadoopCatalog admin = new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), warehouse.getAbsolutePath());
        TableIdentifier id = TableIdentifier.of(Namespace.of("db"), name);
        PartitionSpec spec = partitioned ? PartitionSpec.builderFor(SCHEMA).identity("name").build() : PartitionSpec.unpartitioned();
        return admin.createTable(id, SCHEMA, spec);
    }

    static Table load(String name) {
        HadoopCatalog admin = new HadoopCatalog(new org.apache.hadoop.conf.Configuration(), warehouse.getAbsolutePath());
        return admin.loadTable(TableIdentifier.of(Namespace.of("db"), name));
    }

    static List<String> rows(Table t, org.apache.iceberg.expressions.Expression filter) throws Exception {
        List<String> out = new ArrayList<>();
        try (CloseableIterable<Record> it = IcebergGenerics.read(t).where(filter).build()) {
            for (Record r : it) {
                out.add(r.getField("id") + ":" + r.getField("name"));
            }
        }
        out.sort(null);
        return out;
    }

    static long dataFilesOnDisk(String name) throws Exception {
        File dir = new File(warehouse, "db/" + name);
        try (Stream<java.nio.file.Path> s = Files.walk(dir.toPath())) {
            return s.filter(p -> p.toFile().isFile())
                    .filter(p -> !p.toString().contains("/metadata/"))
                    .filter(p -> !p.getFileName().toString().startsWith("."))
                    .count();
        }
    }

    static Map<String, Object> row(Object id, String name) {
        Map<String, Object> m = new HashMap<>();
        m.put("id", id);
        m.put("name", name);
        return m;
    }

    static DefaultCamelContext camel(IcebergCatalog cat, int redeliveries) throws Exception {
        DefaultCamelContext ctx = new DefaultCamelContext();
        IcebergComponent comp = new IcebergComponent();
        Field f = IcebergComponent.class.getDeclaredField("catalog"); // no setter exists (see C1)
        f.setAccessible(true);
        f.set(comp, cat);
        ctx.addComponent("iceberg", comp);
        ctx.addRoutes(new RouteBuilder() {
            public void configure() {
                errorHandler(defaultErrorHandler().maximumRedeliveries(redeliveries).redeliveryDelay(0));
                from("direct:in").to("iceberg:sink");
            }
        });
        ctx.start();
        return ctx;
    }

    static Exchange send(DefaultCamelContext ctx, String table, Object body) {
        ProducerTemplate pt = ctx.createProducerTemplate();
        return pt.send("direct:in", ex -> {
            if (table != null) {
                ex.getMessage().setHeader("ICEBERG_TABLE", table);
            }
            ex.getMessage().setBody(body);
        });
    }

    static String outcome(Exchange ex) {
        return ex.getException() == null ? "exchange succeeded" : "exchange failed: " + ex.getException();
    }

    public static void main(String[] args) throws Exception {
        variant = args[0];
        warehouse = Files.createTempDirectory("wh-" + variant).toFile();
        System.out.println("variant=" + variant + "  producer class from "
                + org.apache.iceberg.camel.IcebergProducer.class.getProtectionDomain().getCodeSource().getLocation());

        if (variant.equals("orig")) {
            System.out.println("[C1] configuring the component's catalog the Camel way");
            DefaultCamelContext c0 = new DefaultCamelContext();
            IcebergComponent comp = new IcebergComponent();
            c0.addComponent("iceberg", comp);
            String r;
            try {
                boolean bound = PropertyBindingSupport.build().withCamelContext(c0).withTarget(comp)
                        .withProperty("catalog", icebergCatalog(false)).withMandatory(true).bind();
                r = "bound=" + bound;
            } catch (Exception e) {
                r = e.getClass().getSimpleName() + ": " + e.getMessage();
            }
            Field f = IcebergComponent.class.getDeclaredField("catalog");
            f.setAccessible(true);
            check("component property 'catalog' cannot be set (no setter)", f.get(comp) == null, r);

            System.out.println("[C2] IcebergCatalog.catalog() (the class as written) under every way to choose the catalog");
            String[][] combos = {
                {"type=hadoop", "hadoop", null},
                {"implementation=HadoopCatalog", null, HadoopCatalog.class.getName()},
                {"type=hadoop + implementation=HadoopCatalog", "hadoop", HadoopCatalog.class.getName()}};
            int failed = 0;
            for (String[] c : combos) {
                IcebergCatalog ic = new IcebergCatalog();
                ic.setName("test");
                ic.setType(c[1]);
                ic.setImplementation(c[2]);
                ic.setWarehouseLocation(warehouse.getAbsolutePath());
                ic.setFileIOImplementation("org.apache.iceberg.hadoop.HadoopFileIO");
                ic.setMetricsReporterImplementation("org.apache.iceberg.metrics.LoggingMetricsReporter");
                try {
                    ic.catalog();
                    System.out.println("      " + c[0] + " (all other fields set): OK");
                } catch (Exception e) {
                    failed++;
                    System.out.println("      " + c[0] + " (all other fields set): " + e);
                }
            }
            check("catalog() fails for every configuration", failed == combos.length, failed + "/" + combos.length + " failed");

            createTable("t1", false);
            DefaultCamelContext ctx = camel(icebergCatalog(false), 0);
            System.out.println("[P1] send 2 rows through the producer as written (control: none needed, the table must gain 2 rows)");
            Exchange ex = send(ctx, "db.t1", List.of(row(1, "a"), row(2, "b")));
            Table t = load("t1");
            check("message reported success but nothing written", ex.getException() == null && t.currentSnapshot() == null,
                    outcome(ex) + "; snapshots=" + (t.currentSnapshot() == null ? 0 : 1) + "; rows=" + rows(t, Expressions.alwaysTrue()));

            System.out.println("[P2] send without the ICEBERG_TABLE header");
            Exchange ex2 = send(ctx, null, List.of(row(3, "c")));
            check("missing header gives NullPointerException instead of a clear error",
                    ex2.getException() instanceof NullPointerException, outcome(ex2));
            ctx.stop();

            System.out.println("[P3] call the (unwired) write() as written, 3 times, as redeliveries would");
            createTable("t3", false);
            Method w = org.apache.iceberg.camel.IcebergProducer.class.getDeclaredMethod("write", IcebergCatalog.class,
                    TableIdentifier.class, org.apache.iceberg.FileFormat.class, List.class);
            w.setAccessible(true);
            org.apache.iceberg.camel.IcebergProducer p = new org.apache.iceberg.camel.IcebergProducer(null);
            String err = null;
            for (int i = 0; i < 3; i++) {
                try {
                    w.invoke(p, icebergCatalog(true), TableIdentifier.parse("db.t3"), org.apache.iceberg.FileFormat.PARQUET,
                            List.of(row(1, "a")));
                } catch (java.lang.reflect.InvocationTargetException e) {
                    err = e.getCause().toString();
                }
            }
            Table t3 = load("t3");
            check("write() can never commit (toDataFile on an unclosed writer)", t3.currentSnapshot() == null && err != null, err);
            long orphans = dataFilesOnDisk("t3");
            check("each failed attempt leaves an orphan data file", orphans > 0, orphans + " data files on disk, 0 in the table");
            check("a new catalog per call, never closed", TestCatalog.CREATED.get() == 3 && TestCatalog.CLOSED.get() == 0,
                    "created=" + TestCatalog.CREATED.get() + " closed=" + TestCatalog.CLOSED.get());
            return;
        }

        // variants "close" (first obvious fix only) and "fixed" (control)
        System.out.println("[L1] one row of two has a wrong type (id is \"abc\" for an int column)");
        createTable("l1", false);
        DefaultCamelContext ctx = camel(icebergCatalog(false), 0);
        Exchange e1 = send(ctx, "db.l1", List.of(row("abc", "bad"), row(2, "good")));
        Table t1 = load("l1");
        String readBack;
        boolean corrupt = false;
        List<String> r1 = List.of();
        try {
            r1 = rows(t1, Expressions.alwaysTrue());
            readBack = "rows in table=" + r1;
        } catch (Exception re) {
            corrupt = true;
            readBack = "reading the table now fails: " + re;
        }
        check("bad row: exchange succeeds and the table ends up wrong or unreadable",
                e1.getException() == null && (corrupt || r1.size() < 2),
                outcome(e1) + "; snapshots=" + (t1.currentSnapshot() == null ? 0 : 1) + "; " + readBack);
        ctx.stop();

        System.out.println("[L2] the commit is applied but the reply is lost once; Camel redelivers once");
        createTable("l2", false);
        DefaultCamelContext ctx2 = camel(icebergCatalog(true), 1);
        TestCatalog.LOSE_NEXT_REPLY.set(true);
        Exchange e2 = send(ctx2, "db.l2", List.of(row(1, "a"), row(2, "b")));
        List<String> r2 = rows(load("l2"), Expressions.alwaysTrue());
        check("redelivery appends the rows a second time", r2.size() > 2, outcome(e2) + "; rows in table=" + r2);

        System.out.println("[L3] 3 messages: catalogs built vs closed");
        int before = TestCatalog.CREATED.get();
        for (int i = 0; i < 3; i++) {
            send(ctx2, "db.l2", List.of(row(10 + i, "x")));
        }
        int made = TestCatalog.CREATED.get() - before;
        check("a new catalog per message, never closed", made >= 3, "catalogs created for 3 messages=" + made
                + " closed=" + TestCatalog.CLOSED.get());
        ctx2.stop();

        System.out.println("[L4] partitioned table (identity(name)), rows a and b");
        createTable("l4", true);
        DefaultCamelContext ctx4 = camel(icebergCatalog(false), 0);
        Exchange e4 = send(ctx4, "db.l4", List.of(row(1, "a"), row(2, "b")));
        Table t4 = load("l4");
        String detail = outcome(e4);
        boolean bug = false;
        if (e4.getException() == null && t4.currentSnapshot() != null) {
            List<String> all = rows(t4, Expressions.alwaysTrue());
            List<String> onlyA = rows(t4, Expressions.equal("name", "a"));
            String part = String.valueOf(t4.currentSnapshot().addedDataFiles(t4.io()).iterator().next().partition());
            detail += "; all rows=" + all + "; WHERE name='a' -> " + onlyA + "; data file partition=" + part;
            bug = !onlyA.equals(List.of("1:a"));
        }
        check("partitioned table: rows committed without partition values", bug, detail);
        ctx4.stop();
    }
}
