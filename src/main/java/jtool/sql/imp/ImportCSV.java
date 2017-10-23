/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sql.imp;

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

import jtool.sql.model.Column;
import jtool.sql.model.DataHolder;
import jtool.sql.model.DbType;
import jtool.sql.model.RowHolder;
import jtool.sql.model.config.DbConfig;
import jtool.sql.model.config.ImportConfig;
import jtool.sql.util.JdbcUtil;
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
