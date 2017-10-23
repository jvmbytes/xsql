package jtool.sql.cmd;

import jtool.sql.exp.ExportBatchInsert;
import jtool.sql.model.config.DbConfig;
import jtool.sql.model.config.ExportConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @author wangoo
 * @since 2017-10-23 13:26
 */
@Slf4j
public class ExpBatchInsertCmd {
    public static void main(String[] args) throws Exception {
        String driverName = args[0];
        String linkUrl = args[1];
        String userName = args[2];
        String password = args[3];
        String saveFileName = args[4];
        String batchSql = args[5];

        String batchSqlFilePath = "./" + batchSql;
        String outputFilePath = "./" + saveFileName + ".sql";

        DbConfig dbConfig = new DbConfig(driverName, linkUrl, userName, password);

        ExportConfig config = new ExportConfig(dbConfig);

        config.setQuerySql(batchSql);
        config.setBatchSqlFilePath(batchSqlFilePath);
        config.setFilePath(outputFilePath);

        log.info("batch exp config:{}", config);
        ExportBatchInsert.export(config);
        log.info("------------------------");
        log.info("over");
    }
}
