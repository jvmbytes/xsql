/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sql.exp;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import jtool.sql.model.Column;
import jtool.sql.model.DbType;
import jtool.sql.model.config.DbConfig;
import jtool.sql.model.config.ExportConfig;
import jtool.sql.util.JdbcUtil;
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
