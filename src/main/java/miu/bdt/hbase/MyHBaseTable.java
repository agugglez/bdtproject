package miu.bdt.hbase;

import miu.bdt.model.MyNewsObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.IOException;
import java.io.Serializable;

// Since this class will be used by Spark, it needs to be serializable
public class MyHBaseTable implements Serializable {

    private static final long serialVersionUID = 8384527959901546660L;

    // private static final String TABLE_NAME = "news";
    private String tableName;
    private static final String CF = "main";
    private static final String TITLE_COL = "Title";
    private static final String TEXT_COL = "Text";

    public MyHBaseTable(String tableName){
        this.tableName = tableName;
    }

    private void setUpHBaseTable() throws IOException {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin())
        {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            table.addFamily(new HColumnDescriptor(CF).setCompressionType(Compression.Algorithm.NONE));

            System.out.println("Checking if HBase table exists...");
            if (admin.tableExists(table.getTableName())) {
                System.out.println("It does! Let's delete it first");
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }

            System.out.println("Creating/recreating HBase table...");
            admin.createTable(table);
            System.out.println("Done!");
        }
    }

    public void streamBulkPut(JavaSparkContext jsc, JavaDStream<MyNewsObject> putDStream) throws IOException {
        Configuration conf = HBaseConfiguration.create();
//        conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
//        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
//        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));

        // delete and re-create the table
        setUpHBaseTable();

        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

        hbaseContext.streamBulkPut(putDStream, TableName.valueOf(tableName), new PutFunction());
    }

    public class PutFunction implements Function<MyNewsObject, Put> {
        private static final long serialVersionUID = 8038080193924048202L;

        public Put call(MyNewsObject n) throws Exception {
            byte[] row = Bytes.toBytes(n.getId());
            Put put = new Put(row);

            // title
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes(TITLE_COL), Bytes.toBytes(n.getTitle()));
            // text
            put.addColumn(Bytes.toBytes(CF), Bytes.toBytes(TEXT_COL), Bytes.toBytes(n.getText()));

            return put;
        }
    }
}
