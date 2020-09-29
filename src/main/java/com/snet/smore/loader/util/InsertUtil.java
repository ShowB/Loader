package com.snet.smore.loader.util;

import com.snet.smore.common.constant.Constant;
import com.snet.smore.common.domain.DbInfo;
import com.snet.smore.common.util.DbUtil;
import com.snet.smore.common.util.EnvManager;
import com.snet.smore.common.util.StringUtil;
import com.snet.smore.loader.domain.DefaultValue;
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

        insertBatch(queue);

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

        Queue<JSONObject> queue = new LinkedList<>();

        for (int i = 0; i < commitSize; i++) {
            if (ListCollection.ERROR_QUEUE.size() < 1)
                break;

            queue.add(ListCollection.ERROR_QUEUE.remove());
        }

        insertOneByOne(queue);
        queue.clear();

        if (ListCollection.ERROR_QUEUE.size() < 1) {
            synchronized (LoaderMain.isSecondaryLoadRunning) {
                LoaderMain.isSecondaryLoadRunning = false;
            }
        }
    }

    private static void insertBatch(Queue<JSONObject> rows) {
        JSONObject jsonByTables = categorizingByTableName(rows);

        String url = EnvManager.getProperty("loader.target.db.url");

        DbInfo dbInfo = new DbInfo();

        dbInfo.setUsername(EnvManager.getProperty("loader.target.db.username"));
        dbInfo.setPassword(EnvManager.getProperty("loader.target.db.password"));
        dbInfo.setClassName(EnvManager.getProperty("loader.target.db.classname"));
        dbInfo.setUrl(url);

        Iterator<Map.Entry<String, List<JSONObject>>> it = jsonByTables.entrySet().iterator();
        Map.Entry<String, List<JSONObject>> next;
        String tableName;
        List<JSONObject> jsons;
        int result = 0;

//        String tableName = EnvManager.getProperty("loader.target.db.table-name");

        Connection conn;
        synchronized (conn = DbUtil.getConnection(dbInfo)) {
            while (it.hasNext()) {
                next = it.next();

                tableName = next.getKey();
                jsons = next.getValue();


                InsertInfo insertInfo = getColumnsAndQuery(conn, url, tableName);

                int i;
                try (PreparedStatement pstmt = conn.prepareStatement(insertInfo.getQuery())) {

                    boolean isSetDefaultValue = false;
                    for (JSONObject row : jsons) {
                        i = 1;

                        for (String key : insertInfo.getColumns()) {
                            if (ListCollection.DEFAULT_VALUES.size() > 0) {
                                for (DefaultValue d : ListCollection.DEFAULT_VALUES) {
                                    if (d.getKey().equalsIgnoreCase(key)) {
                                        isSetDefaultValue = true;
                                        break;
                                    }
                                }
                            }

                            if (isSetDefaultValue) {
                                isSetDefaultValue = false;
                                continue;
                            } else {
                                pstmt.setObject(i, row.get(key));
                            }

                            i++;
                        }

                        pstmt.addBatch();
                        pstmt.clearParameters();
                    }

                    int[] results = pstmt.executeBatch();

                    for (int r : results) {
                        result += r;
                    }

//                    conn.commit();
//                    log.info("[{}] rows have successfully inserted into [{}] : remained queue size = {}", result, tableName, ListCollection.LOAD_QUEUE.size());
//                    LoaderMain.success += result;

                } catch (SQLException e) {
                    try {
                        conn.rollback();
                    } catch (Exception ex) {
                        log.error("An error occurred while rolling back transaction.", e);
                    }

                    ListCollection.ERROR_QUEUE.addAll(rows);

                    log.info("[{}] rows have occurred error while batch inserting. " +
                                    "Secondary insert will be started for these records."
                            , rows.size());

                    break;
                }
            }

            try {
                conn.commit();
                log.info("[{}] rows have successfully inserted. : remained queue size = {}", result, ListCollection.LOAD_QUEUE.size());
                LoaderMain.success += result;
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    private static void insertOneByOne(Queue<JSONObject> rows) {
        JSONObject jsonByTables = categorizingByTableName(rows);

        String url = EnvManager.getProperty("loader.target.db.url");

        DbInfo dbInfo = new DbInfo();

        dbInfo.setUsername(EnvManager.getProperty("loader.target.db.username"));
        dbInfo.setPassword(EnvManager.getProperty("loader.target.db.password"));
        dbInfo.setClassName(EnvManager.getProperty("loader.target.db.classname"));
        dbInfo.setUrl(url);

        Iterator<Map.Entry<String, List<JSONObject>>> it = jsonByTables.entrySet().iterator();
        Map.Entry<String, List<JSONObject>> next;
        String tableName;
        List<JSONObject> jsons;
        int result = 0;

//        String tableName = EnvManager.getProperty("loader.target.db.table-name");

        Connection conn;
        synchronized (conn = DbUtil.getConnection(dbInfo)) {
            while (it.hasNext()) {
                next = it.next();

                tableName = next.getKey();
                jsons = next.getValue();

                InsertInfo insertInfo = getColumnsAndQuery(conn, url, tableName);

                int i;
                boolean isSetDefaultValue = false;

                for (JSONObject row : jsons) {
                    try (PreparedStatement pstmt = conn.prepareStatement(insertInfo.getQuery())) {
                        i = 1;

                        for (String key : insertInfo.getColumns()) {
                            if (ListCollection.DEFAULT_VALUES.size() > 0) {
                                for (DefaultValue d : ListCollection.DEFAULT_VALUES) {
                                    if (d.getKey().equalsIgnoreCase(key)) {
                                        isSetDefaultValue = true;
                                        break;
                                    }
                                }
                            }

                            if (isSetDefaultValue) {
                                isSetDefaultValue = false;
                                continue;
                            } else {
                                pstmt.setObject(i, row.get(key));
                            }


                            i++;
                        }

                        result += pstmt.executeUpdate();
                        pstmt.clearParameters();

                        conn.commit();

                    } catch (Exception e) {
                        try {
                            conn.rollback();
                        } catch (Exception ex) {
                            log.error("An error occurred while rolling back transaction.", e);
                        }
                        log.error("An error occurred while inserting to [{}]. {}", tableName, e.getMessage());
                        LoaderMain.error++;
                    }
                }
            }

            if (result > 0)
                log.info("[{}] rows have successfully inserted. ... by Secondary Insert : remained queue size = {}"
                        , result, ListCollection.ERROR_QUEUE.size());

            LoaderMain.success += result;

        }

    }

    private static InsertInfo getColumnsAndQuery(Connection conn, String url, String tableName) {
        InsertInfo info = new InsertInfo();
        List<String> columns = new ArrayList<>();

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

        boolean isSetDefaultValue = false;
        for (int i = 0; i < columns.size(); i++) {
            if (ListCollection.DEFAULT_VALUES.size() > 0) {
                for (DefaultValue d : ListCollection.DEFAULT_VALUES) {
                    if (d.getKey().equalsIgnoreCase(columns.get(i))) {
                        query.append(d.getValue());
                        isSetDefaultValue = true;
                        break;
                    }
                }
            }

            if (isSetDefaultValue) {
                isSetDefaultValue = false;
                continue;
            } else {
                query.append("?");
            }

//            if ("LOADDT".equals(columns.get(i)))
//                query.append("DATE_FORMAT(CURRENT_TIMESTAMP, '%Y%m%d %H:%i:%s')");
//            else
//                query.append("?");

            if (i < columns.size() - 1)
                query.append(Constant.LINE_SEPARATOR).append(", ");
        }

        query.append(")");

        info.setColumns(columns);
        info.setQuery(query.toString());

        return info;
    }

    private static JSONObject categorizingByTableName(Queue<JSONObject> queue) {
//        Iterator it;
//        String key;

        JSONObject resultJson = new JSONObject();

        String tableNm;
        for (JSONObject j : queue) {
            tableNm = (String) j.get("smore_table_nm");

            if (resultJson.get(tableNm) == null)
                resultJson.put(tableNm, new ArrayList<>());

            ((List) resultJson.get(tableNm)).add(j);

//            it = j.keySet().iterator();
//
//            while (it.hasNext()) {
//                key = (String) it.next();
//
//                if (resultJson.get("key") == null)
//                    resultJson.put(key, new ArrayList<>());
//
//                ((List) resultJson.get(key)).add(j.get(key));
//            }
        }

        return resultJson;
    }

}
