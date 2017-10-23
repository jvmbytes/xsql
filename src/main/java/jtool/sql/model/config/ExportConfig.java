package jtool.sql.model.config;

import jtool.sql.model.ExportFormat;
import lombok.Data;

/**
 * @author wangoo
 * @since 2017-10-23 11:16
 */
@Data
public class ExportConfig {

    private DbConfig dbConfig;

    private String tableName;
    private String querySql;
    private String batchSqlFilePath;
    private String filePath;
    private ExportFormat exportFormat = ExportFormat.csv;

    public ExportConfig() {
    }

    public ExportConfig(DbConfig dbConfig) {
        this.dbConfig = dbConfig;
    }
}
