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

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

public class IcebergCatalog {

    private String name;
    private String type;
    private String implementation;
    private String fileIOImplementation;
    private String warehouseLocation;
    private String metricsReporterImplementation;
    private boolean cacheEnabled;
    private boolean cacheCaseSensitive;
    private long cacheExpirationIntervalMillis;
    private Configuration configuration;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getImplementation() {
        return implementation;
    }

    public void setImplementation(String implementation) {
        this.implementation = implementation;
    }

    public String getFileIOImplementation() {
        return fileIOImplementation;
    }

    public void setFileIOImplementation(String fileIOImplementation) {
        this.fileIOImplementation = fileIOImplementation;
    }

    public String getWarehouseLocation() {
        return warehouseLocation;
    }

    public void setWarehouseLocation(String warehouseLocation) {
        this.warehouseLocation = warehouseLocation;
    }

    public String getMetricsReporterImplementation() {
        return metricsReporterImplementation;
    }

    public void setMetricsReporterImplementation(String metricsReporterImplementation) {
        this.metricsReporterImplementation = metricsReporterImplementation;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    public boolean isCacheCaseSensitive() {
        return this.cacheCaseSensitive;
    }

    public void setCacheCaseSensitive(boolean cacheCaseSensitive) {
        this.cacheCaseSensitive = cacheCaseSensitive;
    }

    public long getCacheExpirationIntervalMillis() {
        return this.cacheExpirationIntervalMillis;
    }

    public void setCacheExpirationIntervalMillis(long cacheExpirationIntervalMillis) {
        this.cacheExpirationIntervalMillis = cacheExpirationIntervalMillis;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Map<String, String> properties() {
        Map<String, String> answer = new HashMap<>();
        answer.put(CatalogUtil.ICEBERG_CATALOG_TYPE, getType());
        answer.put(CatalogProperties.CATALOG_IMPL, getImplementation());
        answer.put(CatalogProperties.FILE_IO_IMPL, getFileIOImplementation());
        answer.put(CatalogProperties.WAREHOUSE_LOCATION, getWarehouseLocation());
        answer.put(CatalogProperties.METRICS_REPORTER_IMPL, getMetricsReporterImplementation());
        answer.put(CatalogProperties.CACHE_ENABLED, Boolean.toString(isCacheEnabled()));
        answer.put(CatalogProperties.CACHE_CASE_SENSITIVE, Boolean.toString(isCacheCaseSensitive()));
        answer.put(CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS, Long.toString(getCacheExpirationIntervalMillis()));
        return answer;
    }

    public Catalog catalog() {
        return CatalogUtil.buildIcebergCatalog(getName(), properties(), (getConfiguration() != null) ? getConfiguration() : new Configuration());
    }

}
