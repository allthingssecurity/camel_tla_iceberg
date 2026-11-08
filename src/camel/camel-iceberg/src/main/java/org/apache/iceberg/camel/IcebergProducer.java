/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.camel;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

public class IcebergProducer extends DefaultProducer {

    public IcebergProducer(IcebergEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String table = exchange.getMessage().getHeader("ICEBERG_TABLE", String.class);
        if (table.isEmpty()) {
            throw new IllegalArgumentException("ICEBERG_TABLE header is empty");
        }

        TableIdentifier tableIdentifier = TableIdentifier.parse(table);

        FileFormat fileFormat = FileFormat.PARQUET;
        if (exchange.getMessage().getHeader("ICEBERG_FILE_FORMAT") != null) {
            fileFormat = FileFormat.fromString(exchange.getMessage().getHeader("ICEBERG_FILE_FORMAT", String.class));
        }

        // TODO write()
        IcebergComponent component = getEndpoint().getComponent(IcebergComponent.class);

    }

    private void write(IcebergCatalog icebergCatalog, TableIdentifier tableIdentifier, FileFormat fileFormat, List<Map<String, Object>> rows) throws Exception {
        Catalog catalog = icebergCatalog.catalog();

        Table table = catalog.loadTable(tableIdentifier);

        String filename = table.location() + "/" + UUID.randomUUID().toString();
        OutputFile outputFile = table.io().newOutputFile(filename);

        DataWriter dataWriter;

        switch (fileFormat) {
            case AVRO:
                dataWriter = Avro.writeData(outputFile)
                        .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
                        .schema(table.schema())
                        .withSpec(table.spec())
                        .overwrite()
                        .build();
                break;
            case PARQUET:
                dataWriter = Parquet.writeData(outputFile)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .schema(table.schema())
                        .withSpec(table.spec())
                        .overwrite()
                        .build();
                break;
            case ORC:
                throw new UnsupportedOperationException("ORC file format not current supported");
            default:
                throw new RuntimeException("Unknown file format: " + fileFormat);
        }

        for (Map<String, Object> row : rows) {
            GenericRecord record = GenericRecord.create(table.schema());
            for (String name : row.keySet()) {
                record.setField(name, row.get(name));
            }

            try {
                dataWriter.write(record);
            } catch (Exception e) {
                // TODO close the writer
            }
        }

        table.newAppend().appendFile(dataWriter.toDataFile()).commit();
    }

}
