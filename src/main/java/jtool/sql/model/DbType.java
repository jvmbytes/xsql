package jtool.sql.model;

/**
 * @author wangoo
 * @since 2017-10-20 11:45
 */
public enum DbType {

    oracle, mysql, unknown;

    public static DbType getDbTypeFromDriverName(String driver) {
        driver = driver.toLowerCase();
        if (driver.contains("mysql")) {
            return mysql;
        } else if (driver.contains("oracle")) {
            return oracle;
        } else {
            return unknown;
        }
    }
}
