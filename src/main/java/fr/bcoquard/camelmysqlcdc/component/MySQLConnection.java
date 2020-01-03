package fr.bcoquard.camelmysqlcdc.component;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.TreeMap;

public final class MySQLConnection implements Closeable {

    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private Connection connection;

    public MySQLConnection(String hostname, int port, String username, String password) throws ClassNotFoundException, SQLException {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        Class.forName("com.mysql.jdbc.Driver");
        connect();
    }

    private void connect() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:mysql://" + hostname + ":" + port + "?serverTimezone=UTC", username, password);
    }

    public TreeMap<String, TreeMap<String, TreeMap<Integer, String>>> queryTablesDefinition() throws SQLException {
        TreeMap<String, TreeMap<String, TreeMap<Integer, String>>> tree = new TreeMap<>();

        StringBuilder sqlBuilder = new StringBuilder()
                .append("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, ")
                .append("DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, ")
                .append("CHARACTER_SET_NAME, COLLATION_NAME from INFORMATION_SCHEMA.columns ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION");

        Statement statement = connection.createStatement();
        try {
            ResultSet rs = statement.executeQuery(sqlBuilder.toString());

            String previousTableSchema = "";
            String previousTableName = "";

            while (rs.next()) {
                String tableSchema = rs.getString("TABLE_SCHEMA");
                String tableName = rs.getString("TABLE_NAME");
                Integer position = rs.getInt("ORDINAL_POSITION");
                String columnName = rs.getString("COLUMN_NAME");


                if (!tableSchema.equals(previousTableSchema)) {
                    tree.put(tableSchema, new TreeMap<>());
                }

                if (!tableName.equals(previousTableName)) {
                    tree.get(tableSchema).put(tableName, new TreeMap<>());
                }

                tree.get(tableSchema).get(tableName).put(position, columnName);

                previousTableSchema = tableSchema;
                previousTableName = tableName;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        statement.close();

        return tree;
    }

    @Override
    public void close() throws IOException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public void reconnect() throws IOException, SQLException {
        close();
        connect();
    }
}
