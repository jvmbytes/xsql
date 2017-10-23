package jtool.sql.model.config;

import jtool.sql.model.DbType;
import lombok.Data;

/**
 * @author wangoo
 * @since 2017-10-23 11:23
 */
@Data
public class DbConfig {

    private String driver;
    private String linkUrl;
    private String userName;
    private String password;

    public DbConfig() {
    }

    public DbConfig(String driver, String linkUrl, String userName, String password) {
        this.driver = driver;
        this.linkUrl = linkUrl;
        this.userName = userName;
        this.password = password;
    }

    public DbType getDbType() {
        return DbType.getDbTypeFromDriverName(driver);
    }

}
