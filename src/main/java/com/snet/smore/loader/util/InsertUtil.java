package com.snet.smore.loader.util;

import com.snet.smore.common.constant.Constant;
import com.snet.smore.common.domain.DbInfo;
import com.snet.smore.common.util.DbUtil;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.StringUtil;
import com.snet.smore.loader.domain.InsertInfo;
import com.snet.smore.loader.main.LoaderMain;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;

import java.sql.*;
import java.util.*;

@Slf4j
public class InsertUtil {

    public static void load() {
        int commitSize = EnvManager.getProperty("loader.target.db.commit-size", 1000);

        if (ListCollection.LOAD_QUEUE.size() < 1) {
            synchronized (LoaderMain.isLoadRunning) {
                LoaderMain.isLoadRunning = false;
            }

            return;
        }


        synchronized (LoaderMain.isLoadRunning) {
            LoaderMain.isLoadRunning = true;
        }

        Queue<JSONObject> queue = new LinkedList<>();

        for (int i = 0; i < commitSize; i++) {
            if (ListCollection.LOAD_QUEUE.size() < 1)
                break;

            queue.add(ListCollection.LOAD_QUEUE.remove());
        }

        Map<String, List<Map<String, Object>>> devidedMap = divideListByTable(queue);
        queue.clear();

        for (Map.Entry<String, List<Map<String, Object>>> next : devidedMap.entrySet()) {
            insertBatch(next.getKey(), next.getValue());
        }
        devidedMap.clear();

        if (ListCollection.LOAD_QUEUE.size() < 1) {
            synchronized (LoaderMain.isLoadRunning) {
                LoaderMain.isLoadRunning = false;
            }
        }
    }

    public static void secondaryLoad() {
        int commitSize = EnvManager.getProperty("loader.target.db.commit-size", 1000);

        if (ListCollection.ERROR_QUEUE.size() < 1) {
            synchronized (LoaderMain.isSecondaryLoadRunning) {
                LoaderMain.isSecondaryLoadRunning = false;
            }

            return;
        }

        synchronized (LoaderMain.isSecondaryLoadRunning) {
            LoaderMain.isSecondaryLoadRunning = true;
        }

        Queue<Map<String, List<Map<String, Object>>>> queue = new LinkedList<>();

        for (int i = 0; i < commitSize; i++) {
            if (ListCollection.ERROR_QUEUE.size() < 1)
                break;

            queue.add(ListCollection.ERROR_QUEUE.remove());
        }

        for (Map<String, List<Map<String, Object>>> map : queue) {
            for (Map.Entry<String, List<Map<String, Object>>> next : map.entrySet()) {
                insertOneByOne(next.getKey(), next.getValue());
            }
        }
        queue.clear();

        if (ListCollection.ERROR_QUEUE.size() < 1) {
            synchronized (LoaderMain.isSecondaryLoadRunning) {
                LoaderMain.isSecondaryLoadRunning = false;
            }
        }
    }

    private static void insertBatch(String tableName, List<Map<String, Object>> rows) {
        String url = EnvManager.getProperty("loader.target.db.url");

        DbInfo dbInfo = new DbInfo();

        dbInfo.setUsername(EnvManager.getProperty("loader.target.db.username"));
        dbInfo.setPassword(EnvManager.getProperty("loader.target.db.password"));
        dbInfo.setUrl(url);

        Connection conn = DbUtil.getConnection(dbInfo);

        InsertInfo insertInfo = getColumnsAndQuery(conn, url, tableName);

        int i;
        try (PreparedStatement pstmt = conn.prepareStatement(insertInfo.getQuery())) {

            for (Map<String, Object> row : rows) {
                i = 1;

                for (String key : insertInfo.getColumns()) {
                    if ("LOADDT".equals(key))
                        continue;
                    else
                        pstmt.setObject(i, row.get(key));

                    i++;
                }

                pstmt.addBatch();
//                pstmt.clearParameters();
            }

            int[] results = pstmt.executeBatch();

            int result = 0;
            for (int r : results) {
                result += r;
            }

            log.info("[{}] rows have successfully inserted into [{}] : remained queue size = {}", result, tableName, ListCollection.LOAD_QUEUE.size());

        } catch (SQLException e) {
            Map<String, List<Map<String, Object>>> errorMap = new HashMap<>();
            errorMap.put(tableName, rows);

            ListCollection.ERROR_QUEUE.add(errorMap);

            log.info("[{}] rows have occurred error while batch inserting. " +
                            "Secondary insert will be started for these records."
                    , rows.size());
//            log.error("An error occurred while connecting database.", e);
        }
    }

    private static void insertOneByOne(String tableName, List<Map<String, Object>> rows) {
        String url = EnvManager.getProperty("loader.target.db.url");

        DbInfo dbInfo = new DbInfo();

        dbInfo.setUsername(EnvManager.getProperty("loader.target.db.username"));
        dbInfo.setPassword(EnvManager.getProperty("loader.target.db.password"));
        dbInfo.setUrl(url);

        Connection conn = DbUtil.getConnection(dbInfo);

        InsertInfo insertInfo = getColumnsAndQuery(conn, url, tableName);

        int i;
        int result = 0;
        for (Map<String, Object> row : rows) {
            try (PreparedStatement pstmt = conn.prepareStatement(insertInfo.getQuery())) {
                i = 1;

                for (String key : insertInfo.getColumns()) {
                    if ("LOADDT".equals(key))
                        continue;
                    else
                        pstmt.setObject(i, row.get(key));

                    i++;
                }

                result += pstmt.executeUpdate();
//                pstmt.clearParameters();

            } catch (SQLException e) {
                log.error("An error occurred while inserting to [{}]. {}", tableName, e.getMessage());
            }
        }

        if (result > 0)
            log.info("[{}] rows have successfully inserted into [{}] ... by Secondary Insert : remained queue size = {}"
                    , result, tableName, ListCollection.ERROR_QUEUE.size());
    }

    private static Map<String, List<Map<String, Object>>> divideListByTable(Queue<JSONObject> queue) {
        Iterator it;
        String key;

        Map<String, List<Map<String, Object>>> map = new HashMap<>();
        for (JSONObject json : queue) {
            it = json.keySet().iterator();

            while (it.hasNext()) {
                key = (String) it.next();
                if (map.get(key) == null)
                    map.put(key, new LinkedList<>());


                map.get(key).add((Map<String, Object>) json.get(key));
            }

        }

        return map;
    }

    private static InsertInfo getColumnsAndQuery(Connection conn, String url, String tableName) {
        InsertInfo info = new InsertInfo();
        List<String> columns = new LinkedList<>();

        String catalog;
        try {
            catalog = url.substring(url.lastIndexOf("/") + 1);

            if (StringUtil.isBlank(catalog))
                catalog = null;
        } catch (Exception e) {
            catalog = null;
        }

        try {
            DatabaseMetaData metaData = conn.getMetaData();

            ResultSet rs = metaData.getColumns(catalog, null, tableName, null);

            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            log.error("An error occurred while generating insert query.", e);
        }

        StringBuilder query = new StringBuilder();
        query.append("INSERT INTO ").append(tableName).append(" (").append(Constant.LINE_SEPARATOR);


        for (int i = 0; i < columns.size(); i++) {
            query.append(columns.get(i));

            if (i < columns.size() - 1)
                query.append(Constant.LINE_SEPARATOR).append(", ");
        }

        query.append(") VALUES (").append(Constant.LINE_SEPARATOR);

        for (int i = 0; i < columns.size(); i++) {
            if ("LOADDT".equals(columns.get(i)))
                query.append("DATE_FORMAT(CURRENT_TIMESTAMP, '%Y%m%d %H:%i:%s')");
            else
                query.append("?");

            if (i < columns.size() - 1)
                query.append(Constant.LINE_SEPARATOR).append(", ");
        }

        query.append(")");

        info.setColumns(columns);
        info.setQuery(query.toString());

        return info;
    }

}
