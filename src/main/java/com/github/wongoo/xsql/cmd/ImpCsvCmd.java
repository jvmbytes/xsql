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

package com.github.wongoo.xsql.cmd;

import com.github.wongoo.xsql.imp.ImportCSV;
import com.github.wongoo.xsql.model.config.DbConfig;
import com.github.wongoo.xsql.model.config.ImportConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * @author wangoo
 * @since 2017-10-23 13:14
 */
@Slf4j
public class ImpCsvCmd {

    @SuppressWarnings("DuplicatedCode")
    public static void main(String[] args) throws Exception {
        String driverName = args[0];
        String linkUrl = args[1];
        String userName = args[2];
        String password = args[3];
        String tableName = args[4];
        String cvsFilePath = args[5];
        String fileEncode = args[6];
        if (StringUtils.isBlank(fileEncode)) {
            fileEncode = "UTF-8";
        }
        String bulkinsertStr = args[7];
        boolean bulkinsert = false;
        if (StringUtils.isBlank(bulkinsertStr)) {
            bulkinsert = Boolean.valueOf(bulkinsertStr);
        }

        DbConfig dbConfig = new DbConfig(driverName, linkUrl, userName, password);

        ImportConfig config = new ImportConfig(dbConfig);

        config.setBulkInsert(bulkinsert);
        config.setTableName(tableName);
        config.setFilePath(cvsFilePath);

        log.info("----------------------");
        log.info("import config:{}", config);

        ImportCSV.imp(config);

        log.info("------------------------");
        log.info("over");
    }
}
