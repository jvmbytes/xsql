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

/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package com.github.wongoo.xsql.imp;

import com.github.wongoo.xsql.model.Column;
import com.github.wongoo.xsql.model.DataHolder;
import com.github.wongoo.xsql.model.DbType;
import com.github.wongoo.xsql.model.RowHolder;
import com.github.wongoo.xsql.model.config.DbConfig;
import com.github.wongoo.xsql.model.config.ImportConfig;
import com.github.wongoo.xsql.util.JdbcUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * It's just for oracle right now!
 *
 * @author Geln Yang
 * @version 1.0
 */
@Slf4j
public class ImportCSV {

    public static void imp(ImportConfig config) throws Exception {
        DbConfig dbConfig = config.getDbConfig();
        Class.forName(dbConfig.getDriver()).newInstance();
        Connection connection = DriverManager
                .getConnection(dbConfig.getLinkUrl(), dbConfig.getUserName(),
                        dbConfig.getPassword());

        DbType dbType = dbConfig.getDbType();

        Map<String, Column> columnMap =
                JdbcUtil.getColumnMap(connection, dbConfig.getUserName(), config.getTableName());


        log.info("start to parse file ...");
        File file = new File(config.getFilePath());
        Reader reader = new InputStreamReader(new FileInputStream(file), config.getFileEncode());
        CSVParser parser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());
        Map<String, Integer> headerMap = parser.getHeaderMap();
        List<String> columns = new ArrayList<String>();
        columns.addAll(headerMap.keySet());
        columns = JdbcUtil.formatColumnNames(columns);

        log.info("finish to parse file ...");

        log.info("start to build data holder object ...");
        final List<CSVRecord> records = parser.getRecords();
        DataHolder dataHolder = new DataHolder() {

            @Override
            public RowHolder getRow(final int i) {
                return new RowHolder() {

                    @Override
                    public int size() {
                        return records.get(i).size();
                    }

                    @Override
                    public Object get(int columnIndex) {
                        return records.get(i).get(columnIndex);
                    }
                };
            }

            @Override
            public int getSize() {
                return records.size();
            }
        };
        log.info("finish to build data holder object ...");

        if (ImportGlobals.isValidFirst()) {
            boolean valid = ImportUtil
                    .validateData(dbType, connection, dbConfig.getUserName(), config.getTableName(),
                            columns, columnMap, dataHolder);
            if (!valid) {
                return; // STOP when not valid
            }
        }

        if (config.isBulkInsert()) {
            ImportUtil
                    .bulkinsetImp(dbType, connection, dbConfig.getUserName(), config.getTableName(),
                            columns, columnMap, dataHolder);
        } else {
            ImportUtil.onebyoneInsertImp(dbType, connection, config.getTableName(), columns,
                    columnMap, dataHolder);
        }
    }

}
