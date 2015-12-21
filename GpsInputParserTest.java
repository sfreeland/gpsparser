
import org.junit.Assert.*;
import org.junit.*;

import java.sql.*;

public class GpsInputParserTest
{
    private static Connection mConnection;

    @BeforeClass
    public static void setup() throws SQLException
    {
        mConnection = DriverManager.getConnection("jdbc:derby:memory:gip;create=true");
        Statement tableCreation = mConnection.createStatement();
        tableCreation.execute("CREATE TABLE " + GpsInputParser.tableName +
                              " (mac       BIGINT NOT NULL, " +
                              "  msg_num   BIGINT NOT NULL, " +
                              "  lat       DOUBLE NOT NULL, " +
                              "  lon       DOUBLE NOT NULL, " +
                              "  time      DOUBLE NOT NULL, " +
                              "  full_time DOUBLE)");
    }

    @Test
    public void core() throws SQLException
    {
        GpsInputParser parser = null;
        parser = new GpsInputParser(mConnection);

        String[] testRecords = new String[]
            { "0013A20040D87F61 >> <=>€#400572793#a#54#GPS:39.033024;-95.722092#STR:055235.988" };
        for (String testRecord : testRecords)
        {
            parser.processRecord(testRecord);
        }
        Statement check = mConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
        check.execute("SELECT * FROM " + GpsInputParser.tableName);
        ResultSet results = check.getResultSet();
        results.last();
        Assert.assertEquals(1, results.getRow());
        Assert.assertEquals(400572793, results.getLong("mac"));
        Assert.assertEquals(54, results.getLong("msg_num"));
        Assert.assertEquals(39.033024, results.getDouble("lat"), Double.POSITIVE_INFINITY);
        Assert.assertEquals(-95.722092, results.getDouble("lon"), Double.POSITIVE_INFINITY);
        Assert.assertEquals(55235.988, results.getDouble("time"), Double.POSITIVE_INFINITY);
    }
}