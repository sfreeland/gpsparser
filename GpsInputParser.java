
import java.sql.*;

public class GpsInputParser
{
    public static final String tableName = "GpsRecords";

    public GpsInputParser(Connection connection) throws SQLException
    {
        mConnection = connection;
        mInsertStatement = 
            connection.prepareStatement("INSERT INTO " + tableName +
                                        "    (mac, lat, lon, time, msg_num)" +
                                        "    VALUES (?, ?, ?, ?, ?)");
    }

    public void processRecord(String record) throws IllegalArgumentException, SQLException
    {
        // Example record:
        // 0013A20040D87F61 >> <=>€#400572793#a#54#GPS:39.033024;-95.722092#STR:055235.988
        String[] parts = record.split("#");
        // part[0] is leading garbage; part[1] through part[3] are identified by position;
        // subsequent parts are identified by prefix for flexibility

        if (parts.length < 4)
        {
            throw new IllegalArgumentException("Too few fields found in record: " + record);
        }
        Long mac = new Long(parts[1]);
        Long msg_num = new Long(parts[3]);
        Double lat = null, lon = null;
        Double time = null;
        for (int i = 4; i < parts.length; ++i)
        {
            PrefixedField field = new PrefixedField(parts[i]);
            if (field.mType != null)
            {
                switch (field.mType)
                {
                case GPS:
                    String[] subParts = field.mValue.split(";");
                    if (subParts.length != 2)
                    {
                        throw new IllegalArgumentException("Invalid GPS field contents: " + field.mValue);
                    }
                    lat = new Double(subParts[0]);
                    lon = new Double(subParts[1]);
                    break;
                case STR:
                    time = new Double(field.mValue);
                    break;
                }
            }
        }
        if ((lat == null) || (lon == null))
        {
            throw new IllegalArgumentException("No GPS field found in record: " + record);
        }
        if (time == null)
        {
            throw new IllegalArgumentException("No STR field found in record: " + record);
        }
        mInsertStatement.setLong(1, mac.longValue());
        mInsertStatement.setDouble(2, lat.doubleValue());
        mInsertStatement.setDouble(3, lon.doubleValue());
        mInsertStatement.setDouble(4, time.doubleValue());
        mInsertStatement.setLong(5, msg_num.longValue());
        mInsertStatement.execute();
    }

    private static class PrefixedField
    {
        public PrefixedField(String serialized) throws IllegalArgumentException
        {
            String[] parts = serialized.split(":");
            if (parts.length > 0)
            {
                try
                {
                    mType = FieldType.valueOf(parts[0].toUpperCase().trim());
                }
                catch (IllegalArgumentException e)
                {
                    // Unrecgonized field; should be ignored but no error condition
                }
                if (mType != null)
                {
                    if (parts.length != 2)
                    {
                        // Recognized field with no associated value; indicates a problem
                        throw new IllegalArgumentException("Invalid field contents: " + serialized);
                    }
                    else
                    {
                        mValue = parts[1];
                    }
                }
            }
        }

        public enum FieldType { GPS, STR }
        public FieldType mType;
        public String mValue;
    }

    private Connection mConnection;
    private PreparedStatement mInsertStatement;
}