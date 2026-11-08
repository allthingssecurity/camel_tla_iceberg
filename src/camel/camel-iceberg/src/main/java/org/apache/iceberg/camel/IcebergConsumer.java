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

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.SuspendableService;
import org.apache.camel.support.ScheduledPollConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergConsumer extends ScheduledPollConsumer implements SuspendableService {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergConsumer.class);

    public IcebergConsumer(Endpoint endpoint, Processor processor) {
        super(endpoint, processor);
    }

    public IcebergConsumer(Endpoint endpoint, Processor processor, ScheduledExecutorService scheduledExecutorService) {
        super(endpoint, processor, scheduledExecutorService);
    }

    @Override
    public Processor getProcessor() {
        return null;
    }

    @Override
    public Exchange createExchange(boolean autoRelease) {
        return null;
    }

    @Override
    public void releaseExchange(Exchange exchange, boolean autoRelease) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    protected int poll() throws Exception {
        return 0;
    }
}
