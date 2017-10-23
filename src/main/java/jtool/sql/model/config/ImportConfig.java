package jtool.sql.model.config;

import jtool.sql.model.SourceType;
import lombok.Data;

/**
 * @author wangoo
 * @since 2017-10-23 11:16
 */
@Data
public class ImportConfig {

    public static final String DEFAULT_ENCODE = "UTF-8";

    private DbConfig dbConfig;

    private String tableName;
    private SourceType sourceType;
    private String filePath;
    private String fileEncode = DEFAULT_ENCODE;
    private boolean bulkInsert = false;

    public ImportConfig() {
    }

    public ImportConfig(DbConfig dbConfig) {
        this.dbConfig = dbConfig;
    }
}
