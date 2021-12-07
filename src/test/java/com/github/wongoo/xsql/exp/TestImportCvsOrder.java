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

import com.github.wongoo.xsql.imp.ImportCSV;
import com.github.wongoo.xsql.model.SourceType;
import com.github.wongoo.xsql.model.config.DbConfig;
import com.github.wongoo.xsql.model.config.ImportConfig;

public class TestImportCvsOrder {

    public static void main(String[] args) throws Exception {
        ImportConfig config  = new ImportConfig();

        DbConfig dbConfig = new DbConfig();
        dbConfig.setDriver("com.mysql.cj.jdbc.Driver");
        dbConfig.setLinkUrl("jdbc:mysql://localhost:3306/delivery");
        dbConfig.setUserName("root");
        dbConfig.setPassword("p0ss0rd");
        config.setDbConfig(dbConfig);

        config.setBulkInsert(false);
        config.setFileEncode("UTF-8");
        config.setTableName("taobao_order_history");
        config.setSourceType(SourceType.csv);


        // importFile(config,"/Users/gelnyang/Downloads/taobao_order/20210701/20160101-20171231.csv");
        // importFile(config,"/Users/gelnyang/Downloads/taobao_order/20210630/20160901-20200420.csv");
        // importFile(config,"/Users/gelnyang/Downloads/taobao_order/20210630/20140429-20160901.csv");
        importFile(config,"/Users/gelnyang/Downloads/taobao_order/20200304/20160708-20191004.csv");
    }

    private static void importFile(ImportConfig config, String f) throws Exception {
        config.setFilePath(f);
        ImportCSV.imp(config);
    }
}
