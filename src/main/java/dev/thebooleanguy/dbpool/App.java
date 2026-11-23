package dev.thebooleanguy.dbpool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class App {
    public static void main(String[] args) {
        System.out.println("simple-dbpool");

        try {
            DBPool pool = new DBPool(2, new DBConfig());

            Connection c1 = pool.getConnection();
            pool.releaseConnection(c1);

            Connection c2 = pool.getConnection();

            System.out.println(c1 == c2);

            pool.releaseConnection(c2);
            pool.shutdown();
        } catch (InterruptedException | SQLException | IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
