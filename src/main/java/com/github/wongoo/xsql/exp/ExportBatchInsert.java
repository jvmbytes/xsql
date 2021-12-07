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
package com.github.wongoo.xsql.exp;

import com.github.wongoo.xsql.model.Column;
import com.github.wongoo.xsql.model.config.DbConfig;
import com.github.wongoo.xsql.model.config.ExportConfig;
import com.github.wongoo.xsql.util.JdbcUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Geln Yang
 * @version 1.0
 */
@Slf4j
public class ExportBatchInsert {

    public static void export(ExportConfig config) throws Exception {
        DbConfig dbConfig = config.getDbConfig();
        Class.forName(dbConfig.getDriver()).newInstance();
        Connection connection = DriverManager
                .getConnection(dbConfig.getLinkUrl(), dbConfig.getUserName(),
                        dbConfig.getPassword());

        StringBuffer buffer = new StringBuffer();
        List<String> lines = FileUtils
                .readLines(new File(config.getBatchSqlFilePath()), ExportConstants.DEFAULT_ENCODE);

        if (lines != null) {
            for (String sql : lines) {
                Statement myStmt = connection.createStatement();
                ResultSet rs = myStmt.executeQuery(sql);
                ResultSetMetaData rmeta = rs.getMetaData();

                String sqlString = sql.toUpperCase();
                int fromIndex = sqlString.indexOf("FROM");
                sqlString = sqlString.substring(fromIndex + 4).trim();
                int blankIndex = sqlString.indexOf(" ");
                String tableName = sqlString.substring(0, blankIndex);

                Map<String, Column> columnMap =
                        JdbcUtil.getColumnMap(connection, dbConfig.getUserName(), tableName);

                StringBuffer result = SqlExportUtil.exportInsert(rmeta, rs, tableName, columnMap);
                buffer.append("-- " + tableName + ExportConstants.NEW_LINE);
                buffer.append(result + ExportConstants.NEW_LINE);
                rs.close();
                myStmt.close();
            }
        }

        SqlExportUtil.saveToFile(config.getFilePath(), buffer);

        connection.close();
    }
}
