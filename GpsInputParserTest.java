
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
        GpsInputParser parser = new GpsInputParser(mConnection);

        String[] testRecords = new String[]
            { "0013A20040D87F61 >> <=>€#400572793#a#54#GPS:39.033024;-95.722092#STR:055235.988",
              "0013A20040D87F61 >> <=>€#400572793#a#54#GPS:39.033024;-95.722092#STR:055235.988#FOO=bar"
            };
        for (int i = 0; i < testRecords.length; ++i)
        {
            parser.processRecord(testRecords[i]);

            Statement check = mConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                      ResultSet.CONCUR_READ_ONLY);
            check.execute("SELECT * FROM " + GpsInputParser.tableName);
            ResultSet results = check.getResultSet();
            results.last();
            Assert.assertEquals(i + 1, results.getRow());
            Assert.assertEquals(400572793, results.getLong("mac"));
            Assert.assertEquals(54, results.getLong("msg_num"));
            Assert.assertEquals(39.033024, results.getDouble("lat"), Double.POSITIVE_INFINITY);
            Assert.assertEquals(-95.722092, results.getDouble("lon"), Double.POSITIVE_INFINITY);
            Assert.assertEquals(55235.988, results.getDouble("time"), Double.POSITIVE_INFINITY);
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void tooFewFields() throws SQLException
    {
        GpsInputParser parser = new GpsInputParser(mConnection);
        parser.processRecord("0013A20040D87F61 >> <=>€#400572793#a");
    }

    @Test(expected=IllegalArgumentException.class)
    public void malformedMac() throws SQLException
    {
        GpsInputParser parser = new GpsInputParser(mConnection);
        parser.processRecord("0013A20040D87F61 >> <=>€#40057x2793#a#54#GPS:39.033024;-95.722092#STR:055235.988");
    }

    @Test(expected=IllegalArgumentException.class)
    public void malformedMsgNum() throws SQLException
    {
        GpsInputParser parser = new GpsInputParser(mConnection);
        parser.processRecord("0013A20040D87F61 >> <=>€#400572793#a#5x4#GPS:39.033024;-95.722092#STR:055235.988");
    }

    @Test(expected=IllegalArgumentException.class)
    public void missingGps() throws SQLException
    {
        GpsInputParser parser = new GpsInputParser(mConnection);
        parser.processRecord("0013A20040D87F61 >> <=>€#400572793#a#54#STR:055235.988");
    }

    @Test(expected=IllegalArgumentException.class)
    public void incompleteGps() throws SQLException
    {
        GpsInputParser parser = new GpsInputParser(mConnection);
        parser.processRecord("0013A20040D87F61 >> <=>€#400572793#a#54#GPS:39.033024#STR:055235.988");
    }

    @Test(expected=IllegalArgumentException.class)
    public void malformedLat() throws SQLException
    {
        GpsInputParser parser = new GpsInputParser(mConnection);
        parser.processRecord("0013A20040D87F61 >> <=>€#400572793#a#54#GPS:39x033024;-95.722092#STR:055235.988");
    }

    @Test(expected=IllegalArgumentException.class)
    public void malformedLon() throws SQLException
    {
        GpsInputParser parser = new GpsInputParser(mConnection);
        parser.processRecord("0013A20040D87F61 >> <=>€#400572793#a#54#GPS:39.033024;-95x722092#STR:055235.988");
    }

    @Test(expected=IllegalArgumentException.class)
    public void missingStr() throws SQLException
    {
        GpsInputParser parser = new GpsInputParser(mConnection);
        parser.processRecord("0013A20040D87F61 >> <=>€#400572793#a#54#GPS:39.033024;-95.722092");
    }

    @Test(expected=IllegalArgumentException.class)
    public void malformedStr() throws SQLException
    {
        GpsInputParser parser = new GpsInputParser(mConnection);
        parser.processRecord("0013A20040D87F61 >> <=>€#400572793#a#54#GPS:39.033024;-95.722092#STR:055x235.988");
    }
}