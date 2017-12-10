/*
 * MIT License
 *
 * Copyright (c) 2017 Barracks Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.barracks.bigqueryservice.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import io.barracks.bigqueryservice.model.DeviceEventHook;
import io.barracks.bigqueryservice.model.DeviceRequest;
import io.barracks.bigqueryservice.model.GoogleClientSecret;
import io.barracks.bigqueryservice.model.Package;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

@Slf4j
@Component
public class BigQueryClient {

    private BigQuery bigQuery;

    private ObjectMapper mapper;

    private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    @Autowired
    public BigQueryClient(BigQuery bigQuery, ObjectMapper mappper) {
        this.bigQuery = bigQuery;
        this.mapper = mappper;
    }

    public void sendEventToBigQuery(DeviceEventHook deviceEventHook) throws Exception {
        final DeviceRequest deviceRequest = deviceEventHook.getDeviceEvent().getRequest();
        final String projectId = deviceEventHook.getHook().getGoogleClientSecret().getProjectId();
        final String datasetName = "Barracks";
        final DatasetId datasetId = DatasetId.of(projectId, datasetName);
        final String tableName = "ResolveVersions";
        final TableId tableId = TableId.of(projectId, datasetName, tableName);

        final String dateSuffix = dateFormat.format(Calendar.getInstance().getTime());
        bigQuery = buildBigQueryCredentialsAndProjectId(deviceEventHook.getHook().getGoogleClientSecret());

        if (bigQuery.getDataset(datasetId) == null) {
            createBigQueryDataset(datasetId);
        }
        if (bigQuery.getTable(tableId) == null) {
            createBigQueryTable(tableId);
        }

        // We send the data to BigQuery row by row
        deviceRequest.getPackages().forEach(
                aPackage -> {
                    final InsertAllRequest.RowToInsert rowToInsert = buildRowToInsert(deviceRequest, aPackage);

                    final InsertAllResponse response = bigQuery.insertAll(
                            InsertAllRequest.newBuilder(tableId)
                                    .addRow(rowToInsert)
                                    .setTemplateSuffix(dateSuffix)
                                    .build()
                    );
                    if (response.hasErrors()) {
                        for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                            log.error("Error :" + entry);
                        }
                    }
                }
        );
    }

    BigQuery buildBigQueryCredentialsAndProjectId(GoogleClientSecret googleClientSecret) throws IOException {
        return bigQuery.getOptions().toBuilder()
                .setCredentials(ServiceAccountCredentials.fromStream(new ByteArrayInputStream(mapper.writeValueAsBytes(googleClientSecret))))
                .setProjectId(googleClientSecret.getProjectId())
                .build().getService();
    }

    private void createBigQueryDataset(DatasetId datasetId) {
        final DatasetInfo datasetInfo = DatasetInfo.of(datasetId);
        bigQuery.create(datasetInfo);
    }

    private void createBigQueryTable(TableId tableId) throws InterruptedException {
        final List<Field> fields = new ArrayList<>();
        fields.add(Field.of("timestamp", Field.Type.timestamp()));
        fields.add(Field.of("userId", Field.Type.string()));
        fields.add(Field.of("unitId", Field.Type.string()));
        fields.add(Field.of("ipAddress", Field.Type.string()));
        fields.add(Field.of("userAgent", Field.Type.string()));
        fields.add(Field.of("customClientData", Field.Type.string()));

        final Field reference = Field.of("reference", Field.Type.string());
        final Field version = Field.of("version", Field.Type.string());
        fields.add(Field.of("packages", Field.Type.record(reference, version)));


        final Schema schema = Schema.of(fields);
        final TableDefinition tableDefinition = StandardTableDefinition.newBuilder().setSchema(schema).build();
        final TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        bigQuery.create(tableInfo);

        int i = 0;
        while (bigQuery.getTable(tableId) == null && i < 15) {
            Thread.sleep(5);
            i++;
        }
    }

    private InsertAllRequest.RowToInsert buildRowToInsert(DeviceRequest deviceRequest, Package aPackage) {
        final Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("timestamp", Instant.now().toString());
        rowContent.put("userId", deviceRequest.getUserId());
        rowContent.put("unitId", deviceRequest.getUnitId());
        rowContent.put("ipAddress", deviceRequest.getIpAddress() == null ? " " : deviceRequest.getIpAddress());
        rowContent.put("userAgent", deviceRequest.getUserAgent() == null ? " " : deviceRequest.getUserAgent());
        rowContent.put("customClientData", deviceRequest.getCustomClientData().toString());

        final Map<String, Object> recordsContent = new HashMap<>();
        recordsContent.put("reference", aPackage.getReference());
        recordsContent.put("version", aPackage.getVersion().get());
        rowContent.put("packages", recordsContent);

        return InsertAllRequest.RowToInsert.of(rowContent);
    }

}
