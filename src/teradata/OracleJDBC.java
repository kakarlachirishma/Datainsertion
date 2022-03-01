package teradata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.tools.JavaCompiler;

public class OracleJDBC {

    private static Map<String, String> columnDataType = new HashMap<>();

    public Connection getTeraCon(String connurl, String username, String password) {
        Connection conn = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(connurl, username, password);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return conn;
    }

    public void preparecolumsMap(Connection con, String tableName, String dbName) {
        try {

            if (con == null) {
                return;
            }
            String query = "SELECT column_name, data_type, data_length FROM all_tab_columns WHERE table_name ='" + tableName + "' ";
            PreparedStatement stmt = con.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String colName = rs.getString(1);
                String colType = rs.getString(2);
                int colLength = rs.getInt(3);
                columnDataType.put(colName, colType + "#&#" + colLength);
                //	System.out.println("colname=>"+colName+"datatype=>"+colType+"#&#"+colLength);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static LocalDate createRandomDate() {
        int startYear = 1995;
        int endYear = 2021;
        int day = createRandomIntBetween(1, 28);
        int month = createRandomIntBetween(1, 12);
        int year = createRandomIntBetween(startYear, endYear);
        return LocalDate.of(year, month, day);
    }

    public static int createRandomIntBetween(int start, int end) {
        return start + (int) Math.round(Math.random() * (end - start));
    }

    public String makeRandomString(int length) {
        String randomString = "";
        try {
            int leftLimit = 97; // letter 'a'
            int rightLimit = 122; // letter 'z'
            int targetStringLength = length;
            Random random = new Random();
            StringBuilder buffer = new StringBuilder(targetStringLength);
            for (int i = 0; i < targetStringLength; i++) {
                int randomLimitedInt = leftLimit + (int) (random.nextFloat() * (rightLimit - leftLimit + 1));
                buffer.append((char) randomLimitedInt);
            }
            randomString = buffer.toString();

            // System.out.println(randomString);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return randomString;
    }

    public int generateNumber(int len) {
        int randomNumber = 0;
        try {
            Random randomObj = new Random();
            randomNumber = randomObj.nextInt(100);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return randomNumber;

    }

    public String prepareColumnsValues() {
        String columnsquery = "";
        String valuequery = "";
        try {
            for (Map.Entry<String, String> data : columnDataType.entrySet()) {
                //System.out.println(data.getKey().trim()+","+data.getValue());

                String column = data.getKey().trim();
                String columnDetails = data.getValue().trim();

                String columnType = columnDetails.split("#&#")[0].trim();
                int columnLength = Integer.valueOf(columnDetails.split("#&#")[1].trim());

                String value = "";
                columnsquery = columnsquery + column + ",";
                //int len=Integer.parseInt(columnType.substring(columnType.indexOf("(")+1, columnType.indexOf(")")));

                // For char
                if (columnType.equals("CHAR")) {
                    valuequery = valuequery + quote(makeRandomString(columnLength)) + ",";

                } //For VarChar
                else if (columnType.equals("VARCHAR2") || columnType.equals("VARCHAR") || columnType.equals("NVARCHAR2")) {
                    valuequery = valuequery + quote(makeRandomString(columnLength / 2)) + ",";

                } // for Date
                else if (columnType.equals("DATE")) {
                    //	valuequery=valuequery+quote(String.valueOf(createRandomDate()))+",";
                    //	valuequery=valuequery+quote("1994-02-23")+",";
                    valuequery = valuequery + "null,";

                } // for Date
                else if (columnType.equals("TIME")) {
                    valuequery = valuequery + quote("15:09:02") + ",";

                } // for TIMESTAMP(6) WITH LOCAL TIME ZONE
                else if (columnType.equals("TIMESTAMP(6) WITH LOCAL TIME ZONE")) {
                    //valuequery=valuequery+quote(String.valueOf(createRandomDate())+" 11:02:30 -07:00")+",";
                    valuequery = valuequery + "null,";

                } 
                // fortime
                else if (columnType.contains("TIMESTAMP")) {
                    //valuequery=valuequery+quote(String.valueOf(createRandomDate())+" 11:02:30 -07:00")+",";
                    valuequery = valuequery + "null,";

                } 
// INTERVAL YEAR(2) TO MONTH
                else if (columnType.equals("INTERVAL YEAR(2) TO MONTH")) {
                    //valuequery=valuequery+"INTERVAL "+quote("10-2")+" YEAR TO MONTH"+",";
                    valuequery = valuequery + "null,";

                } //for Decimal
                else if (columnType.equals("DECIMAL") || columnType.equals("FLOAT") || columnType.equals("BINARY_DOUBLE")) {
                    valuequery = valuequery + 12.09 + ",";

                } else if (columnType.equals("INTEGER") || columnType.equals("NUMBER")) {
                    valuequery = valuequery + generateNumber(columnLength) + ",";

                } else if (columnType.equals("SMALLINT")) {
                    valuequery = valuequery + generateNumber(columnLength) + ",";

                } else {
                    System.out.println("Need TO handle ->Column Name:-" + column + "| columnType ->" + columnType);

                    valuequery = valuequery + ",";

                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return columnsquery + "#@@#" + valuequery;
    }

    public static String quote(String s) {
        return new StringBuilder()
                .append('\'')
                .append(s)
                .append('\'')
                .toString();
    }

    private boolean insertData(String query, Connection conn) {
        Statement stmt = null;
        boolean flag = false;
        try {

            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            flag = true;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stmt = null;
        }
        return flag;
    }

    public static void main(String[] args) throws Exception {
        String connurl = "jdbc:oracle:thin:@//40.76.57.104:1521/dxc";

        String username = "system";
        String password = "OraPasswd1";
        String tablename = "DC_PERSONNEL";
        String dbName = "ME_DWH";
        

        OracleJDBC teradataJDBC = new OracleJDBC();

        Connection con = teradataJDBC.getTeraCon(connurl, username, password);
        teradataJDBC.preparecolumsMap(con, tablename, dbName);

        for (int i = 0; i < 10; i++) {
            String query = "INSERT INTO " + dbName + "." + tablename + " ";
            String colval = teradataJDBC.prepareColumnsValues();

            String[] str = colval.split("#@@#");

            String columns = "";
            String val = "";
            if (str.length == 2) {
                columns = str[0];
                columns = columns.substring(0, columns.length() - 1);

                val = str[1];
                val = val.substring(0, val.length() - 1);
            }

            query = query + "(" + columns + ") VALUES (" + val + ")";

            System.out.println("----------------------------------------");
            System.out.println(i);
            System.out.println("query :-" + query);

            boolean status = teradataJDBC.insertData(query, con);
            System.out.println("Status :-" + status);

        }

    }

}
