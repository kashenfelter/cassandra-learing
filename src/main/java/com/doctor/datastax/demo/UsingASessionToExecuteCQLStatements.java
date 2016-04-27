package com.doctor.datastax.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * @author sdcuike
 *
 *         Create At 2016年4月27日 下午1:50:55
 * 
 * @see http://docs.datastax.com/en/latest-java-driver/java-driver/quick_start/qsSimpleClientAddSession_t.html
 * 
 *      creating tables
 *      inserting data into those tables
 *      querying the tables
 *      printing the results
 */
public class UsingASessionToExecuteCQLStatements {

    public static void main(String[] args) {
        UsingASessionToExecuteCQLStatements demo = new UsingASessionToExecuteCQLStatements();

        demo.connect("127.0.0.1");
        demo.createSchema();
        demo.loadData();
        demo.querySchema();
        demo.close();

    }

    protected Logger log = LoggerFactory.getLogger(getClass());
    private String address = "127.0.0.1";
    private Session session;
    private Cluster cluster;

    public void connect(String node) {
        cluster = Cluster.builder().addContactPoint(address).build();
        Metadata metadata = cluster.getMetadata();
        log.info("Connected to cluster:{}", metadata.getClusterName());

        for (Host host : metadata.getAllHosts()) {
            log.info("Datacenter:{},Host:{},Rack:{}", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect();
    }

    public void createSchema() {
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS simplex WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1};";

        session.execute(createKeyspace);
        String createTable_songs = "create table if not exists simplex.songs (" +
                "id uuid primary key," +
                "title text," +
                "album text," +
                "artist text," +
                "tags set<text>," +
                "data blob" +
                ");";
        session.execute(createTable_songs);

        String createTable_playlists = "create table if not exists simplex.playlists (" +
                "id uuid," +
                "title text," +
                "album text, " +
                "artist text," +
                "song_id uuid," +
                "primary key (id, title, album, artist)" +
                ");";

        session.execute(createTable_playlists);
    }

    public void loadData() {
        session.execute(
                "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
                        "VALUES (" +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'," +
                        "{'jazz', '2013'})" +
                        ";");
        session.execute(
                "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
                        "VALUES (" +
                        "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
                        "756716f7-2e54-4715-9f00-91dcbea6cf50," +
                        "'La Petite Tonkinoise'," +
                        "'Bye Bye Blackbird'," +
                        "'Joséphine Baker'" +
                        ");");
    }

    public void querySchema() {
        ResultSet results = session.execute("SELECT * FROM simplex.playlists " +
                "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
        System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
                "-------------------------------+-----------------------+--------------------"));
        for (Row row : results) {
            System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
                    row.getString("album"), row.getString("artist")));
        }
        System.out.println();
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

}
