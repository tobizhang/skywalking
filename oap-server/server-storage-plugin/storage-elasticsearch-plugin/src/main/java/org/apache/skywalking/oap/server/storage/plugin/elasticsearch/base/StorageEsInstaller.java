/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base;

import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.skywalking.oap.server.core.storage.StorageException;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.storage.model.ModelColumn;
import org.apache.skywalking.oap.server.core.storage.model.ModelInstaller;
import org.apache.skywalking.oap.server.library.client.Client;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author peng-yongsheng
 */
public class StorageEsInstaller extends ModelInstaller {

    private static final Logger logger = LoggerFactory.getLogger(StorageEsInstaller.class);

    private final int indexShardsNumber;
    private final int indexReplicasNumber;
    private final int indexRefreshInterval;
    private HashMap<String, Integer> indexSpecialCfgs;
    private final ColumnTypeEsMapping columnTypeEsMapping;

    public StorageEsInstaller(ModuleManager moduleManager, int indexShardsNumber, int indexReplicasNumber, int indexRefreshInterval, String indexSpecialCfgs) {
        super(moduleManager);
        this.indexShardsNumber = indexShardsNumber;
        this.indexReplicasNumber = indexReplicasNumber;
        this.indexRefreshInterval = indexRefreshInterval;
        this.indexSpecialCfgs = parseEsIndexCfgs(indexSpecialCfgs);
        this.columnTypeEsMapping = new ColumnTypeEsMapping();
    }

    @Override
    protected boolean isExists(Client client, Model model) throws StorageException {
        ElasticSearchClient esClient = (ElasticSearchClient) client;
        try {
            if (model.isCapableOfTimeSeries()) {
                return esClient.isExistsTemplate(model.getName()) && esClient.isExistsIndex(model.getName());
            } else {
                return esClient.isExistsIndex(model.getName());
            }
        } catch (IOException e) {
            throw new StorageException(e.getMessage());
        }
    }

    @Override
    protected void createTable(Client client, Model model) throws StorageException {
        ElasticSearchClient esClient = (ElasticSearchClient) client;

        JsonObject settings = createSetting(model);
        JsonObject mapping = createMapping(model);
        logger.info("index {}'s columnTypeEsMapping builder str: {}", esClient.formatIndexName(model.getName()), mapping.toString());

        try {
            if (model.isCapableOfTimeSeries()) {
                if (!esClient.isExistsTemplate(model.getName())) {
                    boolean isAcknowledged = esClient.createTemplate(model.getName(), settings, mapping);
                    logger.info("create {} index template finished, isAcknowledged: {}", model.getName(), isAcknowledged);
                    if (!isAcknowledged) {
                        throw new StorageException("create " + model.getName() + " index template failure, ");
                    }
                }
                if (!esClient.isExistsIndex(model.getName())) {
                    String timeSeriesIndexName = TimeSeriesUtils.timeSeries(model);
                    boolean isAcknowledged = esClient.createIndex(timeSeriesIndexName);
                    logger.info("create {} index finished, isAcknowledged: {}", timeSeriesIndexName, isAcknowledged);
                    if (!isAcknowledged) {
                        throw new StorageException("create " + timeSeriesIndexName + " time series index failure, ");
                    }
                }
            } else {
                boolean isAcknowledged = esClient.createIndex(model.getName(), settings, mapping);
                logger.info("create {} index finished, isAcknowledged: {}", model.getName(), isAcknowledged);
                if (!isAcknowledged) {
                    throw new StorageException("create " + model.getName() + " index failure, ");
                }
            }
        } catch (IOException e) {
            throw new StorageException(e.getMessage());
        }
    }

    private JsonObject createSetting(Model model) {
        JsonObject setting = new JsonObject();
        Integer indexShardsNumberTmp = indexSpecialCfgs.get(model.getName() + "_indexShardsNumber");
        if (indexShardsNumberTmp != null) {
            setting.addProperty("index.number_of_shards", indexShardsNumberTmp.intValue());
        } else {
            setting.addProperty("index.number_of_shards", indexShardsNumber);
        }

        Integer indexReplicasNumberTmp = indexSpecialCfgs.get(model.getName() + "_indexReplicasNumber");
        if (indexShardsNumberTmp != null) {
            setting.addProperty("index.number_of_replicas", indexReplicasNumberTmp.intValue());
        } else {
            setting.addProperty("index.number_of_replicas", indexReplicasNumber);
        }

        Integer flushIntervalTmp = indexSpecialCfgs.get(model.getName() + "_flushInterval");
        if (flushIntervalTmp != null) {
            setting.addProperty("index.refresh_interval", model.isRecord() ? TimeValue.timeValueSeconds(10).toString() : TimeValue.timeValueSeconds(flushIntervalTmp.intValue()).toString());
        } else {
            setting.addProperty("index.refresh_interval", model.isRecord() ? TimeValue.timeValueSeconds(10).toString() : TimeValue.timeValueSeconds(indexRefreshInterval).toString());
        }

        setting.addProperty("analysis.analyzer.oap_analyzer.type", "stop");
        return setting;
    }

    private JsonObject createMapping(Model model) {
        JsonObject mapping = new JsonObject();
        mapping.add(ElasticSearchClient.TYPE, new JsonObject());

        JsonObject type = mapping.get(ElasticSearchClient.TYPE).getAsJsonObject();

        JsonObject properties = new JsonObject();
        type.add("properties", properties);

        for (ModelColumn columnDefine : model.getColumns()) {
            if (columnDefine.isMatchQuery()) {
                String matchCName = MatchCNameBuilder.INSTANCE.build(columnDefine.getColumnName().getName());

                JsonObject originalColumn = new JsonObject();
                originalColumn.addProperty("type", columnTypeEsMapping.transform(columnDefine.getType()));
                originalColumn.addProperty("copy_to", matchCName);
                properties.add(columnDefine.getColumnName().getName(), originalColumn);

                JsonObject matchColumn = new JsonObject();
                matchColumn.addProperty("type", "text");
                matchColumn.addProperty("analyzer", "oap_analyzer");
                properties.add(matchCName, matchColumn);
            } else if (columnDefine.isContent()) {
                JsonObject column = new JsonObject();
                column.addProperty("type", "text");
                column.addProperty("index", false);
                properties.add(columnDefine.getColumnName().getName(), column);
            } else {
                JsonObject column = new JsonObject();
                column.addProperty("type", columnTypeEsMapping.transform(columnDefine.getType()));
                properties.add(columnDefine.getColumnName().getName(), column);
            }
        }

        logger.debug("elasticsearch index template setting: {}", mapping.toString());

        return mapping;
    }

    private HashMap<String, Integer> parseEsIndexCfgs(String indexSpecialCfgs) {
        HashMap<String, Integer> esIndexCfgs = new HashMap<>();
        if (StringUtils.isBlank(indexSpecialCfgs)) {
            return esIndexCfgs;
        }
        String[] kvs = indexSpecialCfgs.split(",");
        for (String kv : kvs) {
            if (StringUtils.isBlank(kv)) {
                continue;
            }
            String[] skvs = kv.split("=");
            if (skvs.length != 2) {
                continue;
            }
            esIndexCfgs.put(skvs[0], Integer.valueOf(skvs[1]));
        }
        return esIndexCfgs;
    }
}
