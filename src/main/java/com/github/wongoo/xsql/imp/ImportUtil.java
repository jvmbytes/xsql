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

package com.github.wongoo.xsql.imp;

import com.github.wongoo.xsql.model.Column;
import com.github.wongoo.xsql.model.DataHolder;
import com.github.wongoo.xsql.model.DbType;
import com.github.wongoo.xsql.model.RowHolder;
import com.github.wongoo.xsql.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Geln Yang
 * @version 1.0
 */
@Slf4j
public class ImportUtil {

    private static final String NUMBER_PATTER = "\\-?\\d+(\\.\\d+)?";

    private static final String INT_PATTERN = "\\d+";

    static String dateFormatPattern_1 = "yyyy/MM/dd";

    static SimpleDateFormat dateFormat_1 = new SimpleDateFormat(dateFormatPattern_1);

    static String dateFormatPattern_2 = "yyyy-MM-dd";

    static SimpleDateFormat dateFormat_2 = new SimpleDateFormat(dateFormatPattern_2);

    static String datetimeFormatPattern_1 = "yyyy/MM/dd HH:mm:ss";

    static SimpleDateFormat datetimeFormat_1 = new SimpleDateFormat(datetimeFormatPattern_1);

    static String datetimeFormatPattern_2 = "yyyy-MM-dd HH:mm:ss";

    static SimpleDateFormat datetimeFormat_2 = new SimpleDateFormat(datetimeFormatPattern_2);

    public static String parseObject(String cname, Object cvalue, Column column) {
        if (cvalue instanceof java.util.Date) {
            String type = column.getType().toUpperCase();
            if (type.equals("DATE")) {
                return dateFormat_1.format((java.util.Date)cvalue);
            } else {
                return datetimeFormat_1.format((java.util.Date)cvalue);
            }
        } else {
            return cvalue.toString();
        }
    }

    public static void addColumnValue(DbType dbType, StringBuffer buffer, int rowIndex, String cname, String cvalue,
        Column column) throws Exception {
        Object formatedValue = getSQLFormatedValue(dbType, rowIndex, cname, cvalue, column, false);
        buffer.append(formatedValue.toString());
    }

    public static Object getSQLFormatedValue(DbType dbType, int rowIndex, String cname, String cvalue, Column column,
        boolean bulkinsert) throws Exception {
        if (column == null || cname == null) {
            throw new RuntimeException("Error Row[" + rowIndex + "] can't find column:" + cname);
        }

        String type = column.getType();

        if (cvalue == null || cvalue.trim().length() == 0) {
            if (cvalue != null && ("NUMBER".equals(type) || "INT".equals(type))) {
                cvalue = cvalue.trim();
            }

            if (cvalue == null || "".equals(cvalue)) {
                if (!column.isNullable()) {
                    throw new SQLException(
                        "Error Row[" + rowIndex + "] the value of column[" + cname + "] should not be null!");
                }
                if (bulkinsert) {
                    return null;
                }
                return "null";
            }
        }

        /* remove excel text flag prefix */
        if (cvalue.startsWith("`") && cvalue.length() > 1) {
            cvalue = cvalue.substring(1);
        }

        if (!bulkinsert && cvalue.contains("'")) {
            String rvalue = cvalue.replaceAll("'", "\"");
            log.error("Error Row[" + rowIndex + "] contains single quotation marks in column[" + cname
                + "], change value from [" + cvalue + "] to [" + rvalue + "]");
            cvalue = rvalue;
        }

        cvalue = cvalue.trim();
        if ((cname.contains("_TEL") || cname.contains("PHONE") || cname.contains("MOBILE")) && cvalue.length() > 0) {
            cvalue = cvalue.replaceAll(" ", "");
            cvalue = cvalue.replaceAll("０", "0");
            cvalue = cvalue.replaceAll("１", "1");
            cvalue = cvalue.replaceAll("２", "2");
            cvalue = cvalue.replaceAll("３", "3");
            cvalue = cvalue.replaceAll("４", "4");
            cvalue = cvalue.replaceAll("５", "5");
            cvalue = cvalue.replaceAll("６", "6");
            cvalue = cvalue.replaceAll("７", "7");
            cvalue = cvalue.replaceAll("８", "8");
            cvalue = cvalue.replaceAll("９", "9");
            cvalue = cvalue.replaceAll(",", "");
            cvalue = cvalue.replaceAll("？", "");
            cvalue = cvalue.replaceAll("`", "");
            cvalue = cvalue.replaceAll("—", "-");
            cvalue = cvalue.replaceAll("——", "-");
            cvalue = cvalue.replaceAll("）", ")");
            cvalue = cvalue.replaceAll("（", "(");

            String rvalue = cvalue.replaceAll("[^0-9\\-\\(\\)]", "");
            if (!rvalue.equals(cvalue)) {
                log.warn("Error Row[" + rowIndex + "] wrong tel/mobile/phone number format, change it from [" + cvalue
                    + "] to [" + rvalue + "]");
                cvalue = rvalue;
            }
        }

        if ("NUMBER".equals(type) || "INT".equals(type) || "DECIMAL".equals(type) || "NUMERIC".equals(type)) {
            /* previous update */
            if (cvalue.endsWith("%")) {
                cvalue = "" + Double.parseDouble(cvalue.substring(0, cvalue.length() - 1)) / 100;
            }

            /* start replace */
            String rvalue = cvalue.replaceAll(",", "");
            rvalue = rvalue.replaceAll("百", "00");
            rvalue = rvalue.replaceAll("千", "000");
            rvalue = rvalue.replaceAll("万", "0000");
            if (rvalue.contains("∞")) {
                // String temp = "9999999999999999999999999999999999999";
                // rvalue = rvalue.replaceAll("∞", temp.substring(temp.length()
                // - column.size + column.decimalDigits + 4 + (rvalue.length() -
                // 1)));
                rvalue = "0";
            }

            rvalue = rvalue.replaceAll("[^0-9\\.\\-]", "");

            if (!rvalue.equals(cvalue)) {
                log.warn(
                    "Error Row[" + rowIndex + "] wrong number format, change it from [" + cvalue + "] to [" + rvalue
                        + "]");
                cvalue = rvalue;
            }
            if (ImportGlobals.isChangeNegativeToZero() && cvalue.startsWith("-")) {
                return "0";
            }

            if ("".equals(cvalue)) {
                return null;
            }

            if (cvalue.length() > column.size) {
                throw new SQLException(
                    "Error Row[" + rowIndex + "] the size of the value[" + cvalue + "] of column[" + cname + "] is "
                        + cvalue.length() + ", while the max size is " + column.size);
            }
            int dotIndex = cvalue.indexOf(".");
            if (dotIndex == -1) {
                dotIndex = cvalue.length();
            }
            if (dotIndex > column.getIntegerSize()) {
                throw new SQLException(
                    "Error Row[" + rowIndex + "] the integer size of the value[" + cvalue + "] of column[" + cname
                        + "] is " + dotIndex + ", while the integer max size is " + column.getIntegerSize());
            }
            if ("NUMBER".equals(column) && !cvalue.matches(NUMBER_PATTER)) {
                throw new SQLException("Error Row[" + rowIndex + "] the value[" + cvalue + "] is not a number!");
            }
            if ("INT".equals(column) && !cvalue.matches(INT_PATTERN)) {
                throw new SQLException("Error Row[" + rowIndex + "] the value[" + cvalue + "] is not a integer!");
            }

            return cvalue;
        }
        /* CHAR,VARCHAR,NVARCHAR */
        else if (type.indexOf("CHAR") != -1) {
            if (cvalue.length() > column.size) {
                throw new SQLException(
                    "Error Row[" + rowIndex + "] the length of the value[" + cvalue + "] of column[" + cname + "] is "
                        + cvalue.length() + ", while the max length is " + column.size);
            }
            if (bulkinsert) {
                return cvalue;
            }
            return "\'" + cvalue + "\'";
        } else if (type.indexOf("TIMESTAMP") != -1 || type.indexOf("DATETIME") != -1) {
            return formateTimestampe(dbType, bulkinsert, rowIndex, type, cname, cvalue);
        } else if (type.indexOf("DATE") != -1) {
            return formateDate(dbType, bulkinsert, rowIndex, type, cname, cvalue);
        } else {
            if (bulkinsert) {
                return cvalue;
            }
            return "\'" + cvalue + "\'";
        }
    }

    private static Object formateDate(DbType dbType, boolean bulkinsert, int rowIndex, String type, String cname,
        String cvalue) {
        boolean firstFormat = true;
        try {
            Date date = dateFormat_1.parse(cvalue);
            if (bulkinsert) {
                return new java.sql.Date(date.getTime());
            }
        } catch (ParseException e) {
            /* try other format pattern */
            try {
                Date date = dateFormat_2.parse(cvalue);
                if (bulkinsert) {
                    return new java.sql.Date(date.getTime());
                }
                firstFormat = false;
            } catch (ParseException ex) {

                throw new RuntimeException(
                    "Error Row[" + rowIndex + "] bad date format value[" + cvalue + "] for column " + cname);
            }
        }
        if (dbType.equals(DbType.oracle)) {
            if (firstFormat) {
                return "to_date(\'" + cvalue + "\',\'yyyy/mm/dd\')";
            } else {
                return "to_date(\'" + cvalue + "\',\'yyyy-mm-dd\')";
            }
        } else if (dbType.equals(DbType.mysql)) {
            if (firstFormat) {
                return "str_to_date(\'" + cvalue + "\',\'%Y/%m/%d\')";
            } else {
                return "str_to_date(\'" + cvalue + "\',\'%Y-%m-%d\')";
            }
        } else {
            return cvalue;
        }
    }

    private static Object formateTimestampe(DbType dbType, boolean bulkinsert, int rowIndex, String type, String cname,
        String cvalue) {
        boolean firstFormat = true;
        try {
            Date date = datetimeFormat_1.parse(cvalue);
            if (bulkinsert) {
                return new java.sql.Timestamp(date.getTime());
            }
        } catch (ParseException e) {
            try {
                Date date = datetimeFormat_2.parse(cvalue);
                if (bulkinsert) {
                    return new java.sql.Timestamp(date.getTime());
                }
                firstFormat = false;
            } catch (ParseException ex) {
                throw new RuntimeException(
                    "Error Row[" + rowIndex + "] bad date time format value[" + cvalue + "] for column " + cname);
            }
        }
        if (dbType.equals(DbType.oracle)) {
            if (firstFormat) {
                return "to_date(\'" + cvalue + "\',\'yyyy/mm/dd hh24:mi:ss\')";
            } else {
                return "to_date(\'" + cvalue + "\',\'yyyy-mm-dd hh24:mi:ss\')";
            }
        } else if (dbType.equals(DbType.mysql)) {
            if (firstFormat) {
                return "str_to_date(\'" + cvalue + "\',\'%Y/%m/%d %H:%i:%s\')";
            } else {
                return "str_to_date(\'" + cvalue + "\',\'%Y-%m-%d %H:%i:%s\')";
            }
        } else {
            return cvalue;
        }
    }

    public static void executeSqls(Connection connection, List<String> sqls) throws SQLException {
        connection.setAutoCommit(ImportGlobals.isAutoCommit());
        Statement statement = connection.createStatement();
        int size = sqls.size();
        int successCount = 0;
        int errorCount = 0;
        int duplicatedCount = 0;
        log.info("start execute sql, total size " + size);
        for (int i = 0; i < size; i++) {
            String sql = sqls.get(i);
            try {
                statement.execute(sql);
            } catch (SQLIntegrityConstraintViolationException ve) {
                duplicatedCount++;
                log.trace(ve.getMessage());
            } catch (Exception e) {
                log.error("error to execute sql[" + i + "]: \t" + sql);
                errorCount++;
                if (OracleInsertUtil.hasPkFkError(e.getMessage()) && ImportGlobals.isContinueWhenPkFkError()) {
                    log.error(e.getMessage());
                    continue;
                }
                throw e;
            }

            if (i % ImportGlobals.getBatchsize() == 0) {
                if (i > 0 && !ImportGlobals.isAutoCommit()) {
                    connection.commit();
                }
                log.info("executing progress:" + i + " / " + size);
            }

            successCount++;
        }
        if (!ImportGlobals.isAutoCommit()) {
            connection.commit();
        }
        log.info("executing progress:" + size + " / " + size);
        log.info(
            "success count:" + successCount + ", duplicated count:" + duplicatedCount + ", error count:" + errorCount);
        log.info("================================");
        log.info("finish execute ...");

        statement.close();
    }

    public static void bulkinsetImp(DbType dbType, Connection connection, String userName, String tableName,
        List<String> columns, Map<String, Column> columnMap, DataHolder dataHolder) throws Exception {
        OracleInsertUtil.createBulkInsertProcedure(connection, tableName, columns, columnMap);
        Set<String> primaryKeys = JdbcUtil.getPrimaryKeys(connection, userName, tableName);
        Set<String> primaryKeyValueSet = new HashSet<String>();

        int totalSize = dataHolder.getSize();
        log.info("start to parse data, total data size:" + totalSize);
        int bulkinsertSize = totalSize;
        if (bulkinsertSize > ImportGlobals.getMaxBulksize()) {
            bulkinsertSize = ImportGlobals.getMaxBulksize();
        }
        log.info("initial bulk insert size:" + bulkinsertSize);
        Object[][] dataArr = new Object[columns.size()][bulkinsertSize];
        int exactDataIndex = 0;
        int bulkinsertCount = 0;
        int errorDataCount = 0;
        for (int recordIndex = 0; recordIndex < dataHolder.getSize(); recordIndex++) {
            RowHolder row = dataHolder.getRow(recordIndex);
            String primaryKeyValue = "";
            for (int i = 0; i < columns.size(); i++) {
                String cname = columns.get(i);
                Object cobject = row.get(i);
                String cvalue = cobject != null ? cobject.toString() : null;
                Column column = columnMap.get(cname);

                Object value = ImportUtil.getSQLFormatedValue(dbType, recordIndex, cname, cvalue, column, true);
                dataArr[i][bulkinsertCount] = value;

                if (primaryKeys.contains(cname)) {
                    primaryKeyValue += value.toString();
                }
            }

            primaryKeyValue = primaryKeyValue.trim();
            if (primaryKeyValueSet.contains(primaryKeyValue)) {
                String errorMessage = "Row[" + recordIndex + "] duplicated primay key value:" + primaryKeyValue;
                if (ImportGlobals.isContinueWhenPkFkError()) {
                    log.error(errorMessage);
                    errorDataCount++;
                } else {
                    throw new Exception(errorMessage);
                }
            } else {
                primaryKeyValueSet.add(primaryKeyValue);
                /* increase when no duplicated */
                exactDataIndex++;
                bulkinsertCount++;
            }

            /* reach the data array size */
            if (bulkinsertSize < totalSize && bulkinsertCount == bulkinsertSize) {
                OracleInsertUtil.bulkinsert(connection, tableName, columns, dataArr);
                /* reset data array */
                dataArr = new Object[columns.size()][bulkinsertSize];
                /* reset bulk insert count to zero */
                bulkinsertCount = 0;
            }
        }
        log.info("finish to parse data ...");
        log.info("correct data count:" + exactDataIndex + ", error data count:" + errorDataCount);

        /* check whether there is still data to insert */
        if (bulkinsertCount > 0) {
            if (bulkinsertCount < bulkinsertSize) {
                for (int i = 0; i < dataArr.length; i++) {
                    dataArr[i] = Arrays.copyOfRange(dataArr[i], 0, bulkinsertCount);
                }
            }
            OracleInsertUtil.bulkinsert(connection, tableName, columns, dataArr);
        }

        OracleInsertUtil.dropBulkInsertProcedure(connection, tableName, columns);
        return;
    }

    public static void onebyoneInsertImp(DbType dbType, Connection connection, String tableName, List<String> columns,
        Map<String, Column> columnMap, DataHolder dataHolder) throws Exception {
        log.info("start to build insert sqls ...");
        String insertSqlPrefix = ImportUtil.buildInsertSqlPrefix(tableName, columns, columnMap);

        List<String> sqls = new ArrayList<String>();
        for (int recordIndex = 0; recordIndex < dataHolder.getSize(); recordIndex++) {
            RowHolder row = dataHolder.getRow(recordIndex);
            StringBuffer sqlBuffer = new StringBuffer();
            sqlBuffer.append(insertSqlPrefix);

            for (int j = 0; j < columns.size(); j++) {
                String cname = columns.get(j);
                Column column = columnMap.get(cname);
                String cvalue = ImportUtil.parseObject(cname, row.get(j), column);
                if (j > 0) {
                    sqlBuffer.append(",");
                }
                ImportUtil.addColumnValue(dbType, sqlBuffer, recordIndex, cname, cvalue, column);
            }
            sqlBuffer.append(")");
            String sql = sqlBuffer.toString();

            sqls.add(sql);
        }
        log.info("finish to build insert sqls ...");

        ImportUtil.executeSqls(connection, sqls);
    }

    public static boolean validateData(DbType dbType, Connection connection, String userName, String tableName,
        List<String> columns, Map<String, Column> columnMap, DataHolder dataHolder) throws Exception {
        Set<String> primaryKeys = JdbcUtil.getPrimaryKeys(connection, userName, tableName);
        Set<String> primaryKeyValueSet = new HashSet<String>();

        log.info("start to validate data ...");
        int exactDataIndex = 0;
        int errorDataCount = 0;
        for (int recordIndex = 0; recordIndex < dataHolder.getSize(); recordIndex++) {
            RowHolder row = dataHolder.getRow(recordIndex);
            String primaryKeyValue = "";
            for (int i = 0; i < columns.size(); i++) {
                String cname = columns.get(i);
                String cvalue = (String)row.get(i);
                Column column = columnMap.get(cname);
                try {
                    Object value = ImportUtil.getSQLFormatedValue(dbType, recordIndex, cname, cvalue, column, true);
                    if (primaryKeys.contains(cname)) {
                        primaryKeyValue += value.toString();
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                    errorDataCount++;
                    continue;
                }
            }

            primaryKeyValue = primaryKeyValue.trim();
            if (primaryKeyValueSet.contains(primaryKeyValue)) {
                String errorMessage = "Row[" + recordIndex + "] duplicated primay key value:" + primaryKeyValue;
                log.error(errorMessage);
                errorDataCount++;
            } else {
                primaryKeyValueSet.add(primaryKeyValue);
                exactDataIndex++; // increase when correct
            }
        }
        log.info("finish to validate data ...");
        log.info("correct data count:" + exactDataIndex + ", error data count:" + errorDataCount);

        return errorDataCount == 0;
    }

    @SuppressWarnings("rawtypes")
    public static String buildInsertSqlPrefix(String tableName, List columns, Map<String, Column> columnMap)
        throws Exception {
        String cname = (String)columns.get(0);
        assertColumnExist(columnMap, tableName, cname);
        String insertSqlPrefix = "insert into " + tableName + "(" + cname;
        for (int i = 1; i < columns.size(); i++) {
            cname = (String)columns.get(i);
            assertColumnExist(columnMap, tableName, cname);
            insertSqlPrefix += "," + cname;
        }
        insertSqlPrefix += ") values(";
        return insertSqlPrefix;
    }

    private static void assertColumnExist(Map<String, Column> columnMap, String tableName, String cname)
        throws Exception {
        Column column = columnMap.get(cname);
        if (cname == null || column == null) {
            throw new Exception("No column named " + cname + " in table " + tableName);
        }
    }

}
