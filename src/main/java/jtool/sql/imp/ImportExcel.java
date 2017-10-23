/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sql.imp;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import jtool.excel.ExcelUtil;
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
public class ImportExcel {

    public static void imp(ImportConfig config) throws Exception {
        DbConfig dbConfig = config.getDbConfig();
        Class.forName(dbConfig.getDriver()).newInstance();
        Connection connection = DriverManager
                .getConnection(dbConfig.getLinkUrl(), dbConfig.getUserName(),
                        dbConfig.getPassword());

        DbType dbType = dbConfig.getDbType();

        File file = new File(config.getFilePath());
        int numberOfSheets = ExcelUtil.getNumberOfSheets(file);
        for (int sheetIndex = 0; sheetIndex < numberOfSheets; sheetIndex++) {
            imp(dbType, connection, dbConfig.getUserName(), config.getTableName(), file, sheetIndex,
                    config.isBulkInsert());
        }

        connection.close();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void imp(DbType dbType, Connection connection, String userName, String tableName,
            File file, int sheetIndex, boolean bulkinsert) throws Exception {
        Map<String, Column> columnMap = JdbcUtil.getColumnMap(connection, userName, tableName);

        log.info("start import sheet:" + sheetIndex);
        List<List<Object>> lines = ExcelUtil.readExcelLines(file, sheetIndex, 0);
        if (lines == null || lines.size() < 2) {
            log.info("no content in sheet:" + sheetIndex);
            return;
        }

        List columns = lines.get(0);
        columns = JdbcUtil.formatColumnNames(columns);

        lines.remove(0); // remove title line
        final List<List<Object>> dataList = lines;

        DataHolder dataHolder = new DataHolder() {

            @Override
            public int getSize() {
                return dataList.size();
            }

            @Override
            public RowHolder getRow(int i) {
                final List<Object> list = dataList.get(i);
                return new RowHolder() {

                    @Override
                    public int size() {
                        return list.size();
                    }

                    @Override
                    public Object get(int i) {
                        if (list.size() <= i) {
                            return null;
                        }
                        return list.get(i);
                    }
                };
            }
        };

        if (ImportGlobals.isValidFirst()) {
            boolean valid = ImportUtil
                    .validateData(dbType, connection, userName, tableName, columns, columnMap,
                            dataHolder);
            if (!valid) {
                return; // STOP when not valid
            }
        }

        if (bulkinsert) {
            ImportUtil.bulkinsetImp(dbType, connection, userName, tableName, columns, columnMap,
                    dataHolder);
        } else {
            ImportUtil.onebyoneInsertImp(dbType, connection, tableName, columns, columnMap,
                    dataHolder);
        }
    }

}
