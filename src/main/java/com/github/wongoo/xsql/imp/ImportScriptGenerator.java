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

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Geln Yang
 * @version 1.0
 */
public class ImportScriptGenerator {

    static Pattern fileNamePattern =
            Pattern.compile("\\d+[\\-_]*([A-Z]+[A-Z_0-9]*)[^A-Z0-9_]*[^\\.]*\\.([A-Z]+)",
                    Pattern.CASE_INSENSITIVE);

    public static void generateScript(String dataDir) throws Exception {
        File dir = new File(dataDir);
        String[] files = dir.list();
        for (String file : files) {
            Matcher matcher = fileNamePattern.matcher(file);
            if (!matcher.matches()) {
                throw new Exception("unknow file name format:" + file);
            }
            String tableName = matcher.group(1);
            String suffix = matcher.group(2);

            if (suffix.equalsIgnoreCase("csv")) {
                System.out.println(
                        "java -Xmx4096m jtool.sqlimport.ImportCSV %driver% \"%url%\" %dbuser% %dbpass% "
                                + tableName + " ./csv/" + file + " UTF-8 true");
            } else if (suffix.toLowerCase().contains("xls")) {
                System.out.println(
                        "java -Xmx4096m jtool.sqlimport.ImportExcel %driver% \"%url%\" %dbuser% %dbpass% "
                                + tableName + " ./csv/" + file + " UTF-8 true");
            } else {
                throw new Exception("unknow file suffix:" + file);
            }
        }
    }
}
