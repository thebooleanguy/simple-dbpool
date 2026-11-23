package dev.thebooleanguy.dbpool;

import java.sql.Connection;
import java.sql.SQLException;

public class App {
    public static void main(String[] args) {
        System.out.println("simple-dbpool");

        String jdbcUrl = "jdbc:h2:mem:pooltest;DB_CLOSE_DELAY=-1";
        String username = "sa";
        String password = "";

        try {
            DBPool pool = new DBPool(2, jdbcUrl, username, password);

            Connection c1 = pool.getConnection();
            pool.releaseConnection(c1);

            Connection c2 = pool.getConnection();

            System.out.println(c1 == c2);

            pool.releaseConnection(c2);
            pool.shutdown();
        } catch (InterruptedException | SQLException e) {
            System.err.println(e.getMessage());
        }
    }
}
