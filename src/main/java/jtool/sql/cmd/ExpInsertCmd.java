package jtool.sql.cmd;

import jtool.sql.exp.ExportInsert;
import jtool.sql.model.config.DbConfig;
import jtool.sql.model.config.ExportConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @author wangoo
 * @since 2017-10-23 13:26
 */
@Slf4j
public class ExpInsertCmd {
    public static void main(String[] args) throws Exception {
        String driverName = args[0];
        String linkUrl = args[1];
        String userName = args[2];
        String password = args[3];
        String tableName = args[4];
        String saveFileName = args[5];
        String querySql = args[6];
        String outputFilePath = "./" + saveFileName + ".sql";



        DbConfig dbConfig = new DbConfig(driverName, linkUrl, userName, password);

        ExportConfig config = new ExportConfig(dbConfig);

        config.setQuerySql(querySql);
        config.setTableName(tableName);
        config.setFilePath(outputFilePath);

        log.info("batch exp config:{}", config);
        ExportInsert.export(config);
        log.info("------------------------");
        log.info("over");
    }
}
