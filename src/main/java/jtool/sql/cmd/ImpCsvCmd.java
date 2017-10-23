package jtool.sql.cmd;

import org.apache.commons.lang.StringUtils;

import jtool.sql.imp.ImportCSV;
import jtool.sql.model.config.DbConfig;
import jtool.sql.model.config.ImportConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @author wangoo
 * @since 2017-10-23 13:14
 */
@Slf4j
public class ImpCsvCmd {
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

        DbConfig dbConfig = new DbConfig(driverName,linkUrl,userName,password);

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
