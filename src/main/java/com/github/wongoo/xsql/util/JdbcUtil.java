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

package com.github.wongoo.xsql.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.wongoo.xsql.model.Column;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Geln Yang
 * @version 1.0
 */
@Slf4j
public class JdbcUtil {

    static String dateFormatPattern = "yyyy/MM/dd";
    static String oracleDateFormatPattern = "yyyy/mm/dd";

    static SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatPattern);

    static String datetimeFormatPattern = "yyyy/MM/dd HH:mm:ss";
    static String oracleDatetimeFormatPattern = "yyyy/mm/dd hh24:mi:ss";

    static SimpleDateFormat datetimeFormat = new SimpleDateFormat(datetimeFormatPattern);

    public static Set<String> getPrimaryKeys(Connection connection, String userName,
            String tableName) throws SQLException {
        log.info("start to get table primary keys ...");
        Set<String> columnSet = new HashSet<String>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet rs =
                metaData.getPrimaryKeys(null, userName.toUpperCase(), tableName.toUpperCase());
        while (rs.next()) {
            String name = rs.getString("COLUMN_NAME");
            columnSet.add(name);
        }
        log.info("finish to get table primary keys:" + columnSet);

        return columnSet;
    }

    public static Map<String, Column> getColumnMap(Connection connection, String userName,
            String tableName) throws SQLException {
        log.info("start to get table column definitions ...");
        Map<String, Column> columnMap = new HashMap<String, Column>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columnResultSet =
                metaData.getColumns(null, userName.toUpperCase(), tableName.toUpperCase(), "%");

        while (columnResultSet.next()) {
            String name = columnResultSet.getString("COLUMN_NAME");
            String type = columnResultSet.getString("TYPE_NAME");
            int size = columnResultSet.getInt("COLUMN_SIZE");
            int digits = columnResultSet.getInt("DECIMAL_DIGITS");
            int nullable = columnResultSet.getInt("NULLABLE");

            Column column = new Column();
            column.setName(name);
            column.setType(type);
            column.setSize(size);
            column.setDecimalDigits(digits);
            column.setNullable(nullable > 0);

            columnMap.put(name, column);
        }
        log.info("finish to get table column definitions ...");
        log.info("column count of table " + tableName + ":" + columnMap.size());

        return columnMap;
    }

    /**
     * remove special characters in column names and check duplicated columns
     */
    public static List<String> formatColumnNames(List<String> columns) throws Exception {

        Set<String> columnNameSet = new HashSet<String>();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            String col = column.replaceAll("\\W", "");
            if (columnNameSet.contains(col)) {
                throw new Exception("Exists duplicated column:" + col);
            }
            columnNameSet.add(col);
            columns.set(i, col);
        }
        return columns;
    }

    public static String getData(Map<String, Column> columnMap, ResultSetMetaData rmeta,
            ResultSet rs, int i) throws SQLException {
        String columnName = rmeta.getColumnName(i);
        Column column = columnMap.get(columnName);
        String type = column.getType();
        return getData(type, rs, i);
    }

    public static String getData(String columnType, ResultSet rs, int i) throws SQLException {
        if (columnType.contains("TIMESTAMP")) {
            Timestamp time = rs.getTimestamp(i);
            if (time == null) {
                return null;
            }
            return datetimeFormat.format(time);
        } else if (columnType.contains("DATE")) {
            java.sql.Date date = rs.getDate(i);
            if (date == null) {
                return null;
            }
            return dateFormat.format(date);
        } else {
            return rs.getString(i);
        }
    }

    public static String oracleInsertFormatedData(String columnType, String value) {
        if (columnType.contains("TIMESTAMP")) {
            return "to_date('" + value + "','" + oracleDatetimeFormatPattern + "')";
        } else if (columnType.contains("DATE")) {
            return "to_date('" + value + "','" + oracleDateFormatPattern + "')";
        } else {
            return "'" + value + "'";
        }
    }
}
