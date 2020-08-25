package com.snet.smore.loader;

import com.snet.smore.common.domain.DbInfo;
import com.snet.smore.common.util.DbUtil;
import com.snet.smore.common.util.EnvManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class MetaDataTest {
    @Test
    public void test() {
        String url = EnvManager.getProperty("loader.target.db.url");

        DbInfo dbInfo = new DbInfo();

        dbInfo.setUsername(EnvManager.getProperty("loader.target.db.username"));
        dbInfo.setPassword(EnvManager.getProperty("loader.target.db.password"));
        dbInfo.setUrl(url);

        Connection conn = DbUtil.getConnection(dbInfo);

        DatabaseMetaData metaData = null;

        String catalog;

        try {
            catalog = url.substring(url.lastIndexOf("/") + 1);
        } catch (Exception e) {
            catalog = null;
        }

        try {
            metaData = conn.getMetaData();

            if (catalog == null) {
                catalog = metaData.getCatalogs().getString(0);
            }

            ResultSet columns = metaData.getColumns(catalog, null, "TC_KORAIL_C0", null);

            while (columns.next()) {
                System.out.println(columns.getString("COLUMN_NAME"));
//                columns.getString()
            }
        } catch (SQLException e) {
            log.error("An error occurred while generating insert query.", e);
        }
    }
}
