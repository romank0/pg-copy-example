package pgcopy;
/**
 * @author Roman Konoval
 */
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.PGCopyOutputStream;
import org.postgresql.core.BaseConnection;

import au.com.bytecode.opencsv.CSVWriter;
import au.com.bytecode.opencsv.ResultSetHelperService;

public class PgCopyExample {

    static final String SQLITE_JDBC_DRIVER = "org.sqlite.JDBC";
    static final String PG_JDBC_DRIVER = "org.postgresql.Driver";
    static final String SQLITE_URL_TEMPLATE = "jdbc:sqlite:%s";
    static final String PG_URL_TEMPLATE = "jdbc:postgresql://localhost:7532/%s";
    private static final int SQLITE_CACHE_SIZE = 20000;
    private static final String AUDIT_EVENT_FIELDS = "id,type,transaction_id,date_time,by_user,app,client_ip,is_business,msg";
    private static final String AUDIT_EVENT_PROPERTY_FIELDS = "id,event_id,key,value_int,value_string,value_date_time,value_boolean,\"index\"";

    public static void main(String[] args) {
        if (!checkArgs(args)) {
            return;
        }
        String sqliteDbPath = args[0];
        String pgDbName = args[1];
        Connection readConnection = null;
        Connection writeConnection = null;
        long start = 0;
        try {
            //org.postgresql.Driver.setLogLevel(org.postgresql.Driver.DEBUG);
            Class.forName(SQLITE_JDBC_DRIVER);
            Class.forName(PG_JDBC_DRIVER);

            System.out.println("Connecting to database: " + sqliteDbPath);
            readConnection = getSqliteConnection(sqliteDbPath);
            writeConnection = getPgConnection(pgDbName);

            prepareAuditEventTables(writeConnection);

            start = System.nanoTime();

            copyTable(readConnection, writeConnection, AUDIT_EVENT_FIELDS, "audit_event");
            copyTable(readConnection, writeConnection, AUDIT_EVENT_PROPERTY_FIELDS, "audit_event_property");

            writeConnection.commit();

            long finish = System.nanoTime();

            System.out.println("Time: " + TimeUnit.MILLISECONDS.convert(finish - start, NANOSECONDS));

        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection(readConnection);
            closeConnection(writeConnection);
        }
        System.out.println("Done");
    }

    public static void copyTable(Connection readConnection, Connection writeConnection, String fields, String table)
            throws SQLException, IOException {
        Statement stmt = readConnection.createStatement();

        CopyManager copyManager = new CopyManager((BaseConnection) writeConnection);
        CopyIn copyIn = copyManager.copyIn(format("copy %s (%s) FROM STDIN WITH (FORMAT CSV, HEADER false)", table, fields));

        Writer w = new OutputStreamWriter(new PGCopyOutputStream(copyIn));

        CSVWriter writer = new CSVWriter(w);

        ResultSet rs = stmt.executeQuery(format("SELECT %s FROM %s", fields, table));
        writeAll(writer, rs);
        IOUtils.closeQuietly(writer);

        rs.close();
        stmt.close();

    }

    private static void writeAll(CSVWriter writer, ResultSet rs) throws SQLException, IOException {
        ResultSetHelperService resultService = new ResultSetHelperService();
        while (rs.next()) {
            writer.writeNext(resultService.getColumnValues(rs, false), false);
        }
    }

    private static void prepareAuditEventTables(Connection writeConnection) throws SQLException {

        execute(writeConnection, "DROP TABLE IF EXISTS audit_event_property");
        execute(writeConnection, "DROP TABLE IF EXISTS audit_event");
        writeConnection.commit();
        execute(writeConnection, "CREATE TABLE audit_event (                                                                                                                                         \n" +
                "    id serial primary key," +
                "    type varchar(50) NOT NULL," +
                "    transaction_id varchar(36) NOT NULL," +
                "    date_time TIMESTAMP WITH TIME ZONE NOT NULL," +
                "    by_user varchar(50) NULL," +
                "    app varchar(50) NOT NULL," +
                "    client_ip varchar(15) NOT NULL," +
                "    is_business BOOLEAN NOT NULL," +
                "    msg varchar(255)" +
                ")");
        execute(writeConnection, "CREATE TABLE audit_event_property (" +
                "    id SERIAL PRIMARY KEY," +
                "    event_id INTEGER REFERENCES audit_event(id) NOT NULL," +
                "    key VARCHAR(50) NOT NULL," +
                "    value_int INTEGER," +
                "    value_string TEXT," +
                "    value_date_time TIMESTAMP WITH TIME ZONE," +
                "    value_boolean BOOLEAN," +
                "    \"index\" INTEGER" +
                ");");
    }

    private static Connection getPgConnection(String dbName) throws SQLException {
        Connection connection = DriverManager.getConnection(format(PG_URL_TEMPLATE, dbName), dbName, dbName);
        connection.setAutoCommit(false);
        return connection;
    }

    private static void executeQueries(Connection connection, List<String> queriesToTime) throws IOException, SQLException {
        for(String query:queriesToTime) {
            executeQueryFromUrl(connection, query);
        }
    }

    private static void executeQueryFromUrl(Connection connection, String url) throws IOException, SQLException {
        File tmpFile = File.createTempFile("ltx", "");
        FileUtils.copyURLToFile(new URL(url), tmpFile);
        String query = FileUtils.readFileToString(tmpFile);
        log("execute: " + query);
        Long start = System.nanoTime();
        executeSelect(connection, query);
        Long finish = System.nanoTime();
        log("Time: " + TimeUnit.MILLISECONDS.convert(finish - start, NANOSECONDS));
    }

    private static boolean checkArgs(String[] args) {
        if (args.length < 2) {
            System.err.println("not enough arguments: path/to/sqlite/db and pg_db_name should be specified");
            return false;
        }
        return true;
    }

    public static void closeConnection(Connection readConnection) {
        try {
            if (readConnection != null)
                readConnection.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }

    public static Connection getSqliteConnection(String dbPath) throws SQLException {
        Connection connection = DriverManager.getConnection(format(SQLITE_URL_TEMPLATE, dbPath));
        connection.setAutoCommit(false);
        execute(connection, "PRAGMA temp_store=MEMORY");
        execute(connection, "PRAGMA journal_mode = MEMORY");
        execute(connection, "PRAGMA cache_size = " + SQLITE_CACHE_SIZE);
        return connection;
    }

    private static void execute(Connection connection, String query) throws SQLException {
        Statement stmt = connection.createStatement();
        log("executing: " + query);
        stmt.executeUpdate(query);
        stmt.close();
        log("execution: finished");
    }

    public static void log(String message) {
        System.out.println(String.format("%d - %s", System.currentTimeMillis(), message));
    }

    private static void executeSelect(Connection connection, String query) throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute(query);
        stmt.close();
    }
}
