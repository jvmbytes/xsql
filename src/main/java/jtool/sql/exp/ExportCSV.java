/**
 * Created Date: Dec 14, 2011 9:35:31 AM
 */
package jtool.sql.exp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;

import jtool.sql.model.Column;
import jtool.sql.model.config.DbConfig;
import jtool.sql.model.config.ExportConfig;
import jtool.sql.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Geln Yang
 * @version 1.0
 */
@Slf4j
public class ExportCSV {

    public static void export(ExportConfig config) throws Exception {
        DbConfig dbConfig = config.getDbConfig();
        Class.forName(dbConfig.getDriver()).newInstance();
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
