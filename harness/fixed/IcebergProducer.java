package org.apache.iceberg.camel;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

/** Control: the producer with the fixes the model checked ("Fixed" mode). Test-only. */
public class IcebergProducer extends DefaultProducer {

    static final String EXCHANGE_ID = "camel.exchange-id";
    private Catalog catalog; // one catalog per producer, not one per message

    public IcebergProducer(IcebergEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String table = exchange.getMessage().getHeader("ICEBERG_TABLE", String.class);
        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("ICEBERG_TABLE header is missing or empty");
        }
        FileFormat fileFormat = FileFormat.PARQUET;
        if (exchange.getMessage().getHeader("ICEBERG_FILE_FORMAT") != null) {
            fileFormat = FileFormat.fromString(exchange.getMessage().getHeader("ICEBERG_FILE_FORMAT", String.class));
        }
        IcebergComponent component = getEndpoint().getComponent(IcebergComponent.class);
        synchronized (this) {
            if (catalog == null) {
                catalog = cat(component).catalog();
            }
        }
        write(catalog, TableIdentifier.parse(table), fileFormat, exchange.getMessage().getBody(List.class),
                exchange.getExchangeId());
    }

    private void write(Catalog catalog, TableIdentifier id, FileFormat fileFormat, List<Map<String, Object>> rows,
            String exchangeId) throws Exception {
        Table table = catalog.loadTable(id);
        if (alreadyCommitted(table, exchangeId)) {
            return; // a redelivery after a commit whose outcome was unknown
        }
        if (!table.spec().isUnpartitioned()) {
            throw new UnsupportedOperationException("partitioned tables are not supported yet");
        }
        OutputFile outputFile = table.io().newOutputFile(table.location() + "/data/" + UUID.randomUUID() + "."
                + fileFormat.name().toLowerCase());
        DataWriter<GenericRecord> dataWriter;
        switch (fileFormat) {
            case AVRO:
                dataWriter = Avro.writeData(outputFile).createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
                        .schema(table.schema()).withSpec(table.spec()).overwrite().build();
                break;
            case PARQUET:
                dataWriter = Parquet.writeData(outputFile).createWriterFunc(GenericParquetWriter::buildWriter)
                        .schema(table.schema()).withSpec(table.spec()).overwrite().build();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported file format: " + fileFormat);
        }
        try {
            for (Map<String, Object> row : rows) {
                GenericRecord record = GenericRecord.create(table.schema());
                for (Map.Entry<String, Object> e : row.entrySet()) {
                    record.setField(e.getKey(), e.getValue());
                }
                dataWriter.write(record); // a bad row fails the whole exchange
            }
        } catch (Exception e) {
            dataWriter.close();
            table.io().deleteFile(outputFile.location());
            throw e;
        }
        dataWriter.close();
        table.newAppend().appendFile(dataWriter.toDataFile()).set(EXCHANGE_ID, exchangeId).commit();
    }

    private static boolean alreadyCommitted(Table table, String exchangeId) {
        table.refresh();
        for (Snapshot s : table.snapshots()) {
            if (exchangeId.equals(s.summary().get(EXCHANGE_ID))) {
                return true;
            }
        }
        return false;
    }

    private static IcebergCatalog cat(IcebergComponent c) throws Exception {
        java.lang.reflect.Field f = IcebergComponent.class.getDeclaredField("catalog");
        f.setAccessible(true);
        return (IcebergCatalog) f.get(c);
    }
}
