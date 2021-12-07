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
import com.github.wongoo.xsql.model.config.DbConfig;
import com.github.wongoo.xsql.model.config.ExportConfig;
import com.github.wongoo.xsql.util.JdbcUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Geln Yang
 * @version 1.0
 * @date Dec 14, 2011 9:35:31 AM
 */
@Slf4j
public class ExportCSV {

    @SuppressWarnings("DuplicatedCode")
    public static void export(ExportConfig config) throws Exception {
        DbConfig dbConfig = config.getDbConfig();
        Class.forName(dbConfig.getDriver());
        Connection connection = DriverManager
                .getConnection(dbConfig.getLinkUrl(), dbConfig.getUserName(),
                        dbConfig.getPassword());

        Statement myStmt = connection.createStatement();
        ResultSet rs = myStmt.executeQuery(config.getQuerySql());
        ResultSetMetaData rmeta = rs.getMetaData();
        Map<String, Column> columnMap =
                JdbcUtil.getColumnMap(connection, dbConfig.getUserName(), config.getTableName());
        StringBuffer buffer = SqlExportUtil.exportCSV(rmeta, rs, columnMap);
        SqlExportUtil.saveToFile(config.getFilePath(), buffer);
        rs.close();
        myStmt.close();
        connection.close();
    }
}
