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
 */

package com.github.wongoo.xsql.model.config;

import com.github.wongoo.xsql.model.ExportFormat;
import lombok.Data;

/**
 * @author wangoo
 * @since 2017-10-23 11:16
 */
@Data
public class ExportConfig {

    private DbConfig dbConfig;

    private String tableName;
    private String querySql;
    private String batchSqlFilePath;
    private String filePath;
    private ExportFormat exportFormat = ExportFormat.csv;

    public ExportConfig() {
    }

    public ExportConfig(DbConfig dbConfig) {
        this.dbConfig = dbConfig;
    }
}
