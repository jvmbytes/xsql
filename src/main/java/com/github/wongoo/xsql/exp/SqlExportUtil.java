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

package com.github.wongoo.xsql.exp;

import com.github.wongoo.xsql.model.Column;
import com.github.wongoo.xsql.util.JdbcUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Geln Yang
 * @createdDate 2014年11月21日
 */
@Slf4j
public class SqlExportUtil {

    public static StringBuffer exportCSV(ResultSetMetaData rmeta, ResultSet rs,
            Map<String, Column> columnMap) throws SQLException, IOException {
        int numColumns = rmeta.getColumnCount();
        log.info("column count: " + numColumns);
        log.info("-------------------------------------");
        String columnNames = "";
        for (int i = 1; i <= numColumns; i++) {
            if (i < numColumns) {
                columnNames += rmeta.getColumnName(i) + " , ";
            } else {
                columnNames += rmeta.getColumnName(i);
            }
        }
        log.info(columnNames);
        log.info("-------------------------------------");

        String head = "";
        for (int i = 1; i <= numColumns; i++) {
            head += rmeta.getColumnName(i) + ",";
        }
        head = head.substring(0, head.length() - 1) + "\r\n";

        // output data
        StringBuffer buffer = new StringBuffer();
        buffer.append(head);
        int count = addExportRecords(buffer, columnMap, rmeta, rs, false, "");
        log.info("Record count: " + count);
        return buffer;
    }

    public static StringBuffer exportInsert(ResultSetMetaData rmeta, ResultSet rs, String tableName,
            Map<String, Column> columnMap) throws SQLException, IOException {
        int numColumns = rmeta.getColumnCount();
        log.info("column count: " + numColumns);
        log.info("-------------------------------------");
        String columns = "";
        for (int i = 1; i <= numColumns; i++) {
            columns += rmeta.getColumnName(i);
            if (i < numColumns) {
                columns += " , ";
            }
            if (tableName == null || tableName.trim().length() == 0) {
                tableName = rmeta.getTableName(i);
            }
        }
        log.info(columns);
        log.info("-------------------------------------");
        log.info("Table Name: " + tableName);

        String insertSqlPrefix = "INSERT INTO " + tableName.toUpperCase() + " (";
        for (int i = 1; i <= numColumns; i++) {
            insertSqlPrefix += rmeta.getColumnName(i) + ",";
        }
        insertSqlPrefix = insertSqlPrefix.substring(0, insertSqlPrefix.length() - 1) + ") VALUES(";

        // output data
        StringBuffer buffer = new StringBuffer();
        int count = addExportRecords(buffer, columnMap, rmeta, rs, true, insertSqlPrefix);
        log.info("Record count: " + count);
        return buffer;
    }

    private static int addExportRecords(StringBuffer buffer, Map<String, Column> columnMap,
            ResultSetMetaData rmeta, ResultSet rs, boolean isInsert, String insertSqlPrefix)
            throws SQLException {
        int count = 0;
        int numColumns = rmeta.getColumnCount();
        while (rs.next()) {
            count++;
            if (isInsert) {
                buffer.append(insertSqlPrefix);
            }
            /* loop add columns values */
            for (int i = 1; i <= numColumns; i++) {
                String columnName = rmeta.getColumnName(i);
                Column column = columnMap.get(columnName);
                String type = column.getType();
                String data = JdbcUtil.getData(type, rs, i);
                addExportRecordColumnValue(buffer, data, type, i == numColumns, isInsert);
            }
        }
        return count;
    }

    private static void addExportRecordColumnValue(StringBuffer buffer, String data,
            String columnType, boolean isLast, boolean isInsert) {
        if (data != null) {
            data = data.trim();
            if (isInsert) {
                data = data.replaceAll("'", "\"");
            } else {
                data = data.replaceAll("\"", "'");
            }
            if (isInsert) {
                data = JdbcUtil.oracleInsertFormatedData(columnType, data);
            } else {
                data = "\"" + data + "\"";
            }
        } else {
            data = isInsert ? "null" : "";
        }

        if (isLast) {
            buffer.append(data);
            if (isInsert) {
                buffer.append(");");
            }
            buffer.append("\r\n");
        } else {
            buffer.append(data + ",");
        }
    }

    public static void saveToFile(String outputFilePath, StringBuffer buffer) throws IOException {
        log.info("start to write into file ... ");
        FileUtils.writeStringToFile(new File(outputFilePath), buffer.toString(), "UTF-8");
        log.info("end to write into file ... ");
    }
}
