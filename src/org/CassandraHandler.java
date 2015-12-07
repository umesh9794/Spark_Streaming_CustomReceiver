package org;

/**
 * Created by user on 7/7/15.
 */
        import com.datastax.driver.core.Cluster;
        import com.datastax.driver.core.ResultSet;
        import com.datastax.driver.core.Session;

        import java.io.FileNotFoundException;
        import java.io.IOException;
        import java.io.InputStream;
        import java.util.Properties;

/**
 * Created by uchaudh on 7/3/2015.
 */
public class CassandraHandler {

    public static Session session=null;

    /**
     *
     */
    public void getConnection()
    {
        try {

            Properties prop = new Properties();
            String propFileName = "config.properties";
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            Cluster cluster = Cluster.builder()
                    .addContactPoints(prop.getProperty("cassandra.serverIP"))
                    .build();

            session = cluster.connect(prop.getProperty("cassandra.default.keyspace"));
        }
        catch (IOException ioe)
        {
            ioe.printStackTrace();
        }

    }

    /**
     *
     * @param query
     */
    public void executeQuery(final String query)
    {
        try
        {
            ResultSet rs = session.execute(query);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

    }


    /**
     *
     * @param query
     * @return
     */
    public ResultSet executeSelectQuery(final String query)
    {
        ResultSet rs=null;
        try
        {
            rs = session.execute(query);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        return rs;
    }

}