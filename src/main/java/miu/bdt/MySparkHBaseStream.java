package miu.bdt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


import org.json.JSONObject;

import java.io.IOException;


public class MySparkHBaseStream {
    private static final String TABLE_NAME = "news";
    private static final String CF = "main";
    private static final String TITLE_COL = "Title";
    private static final String TEXT_COL = "Text";

    public static void main(String[] args) throws IOException {
        // create or re-create HBase table
        setUpHBaseTable();

        final String hostname = "localhost";
        final int port = 9999;

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

        // Create a JavaReceiverInputDStream on target ip:port
        // Note that no duplication in storage level only for running locally.
        // Replication necessary in distributed scenario for fault tolerance.
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(hostname,
                port, StorageLevels.MEMORY_AND_DISK_SER);

        JavaDStream<MyNewsObject> newsStream = lines.map(x -> parseJson(x));
//        JavaPairDStream<String, Integer> wordCounts = titles.mapToPair(s -> new Tuple2<>(s, 1))
//                .reduceByKey((i1, i2) -> i1 + i2);

        newsStream.print();
        streamBulkPut(ssc.sparkContext(), TABLE_NAME, newsStream);

        ssc.start();
        ssc.awaitTermination();
    }

    private static void setUpHBaseTable() throws IOException {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin())
        {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF).setCompressionType(Algorithm.NONE));

            System.out.println("Checking if HBase table exists...");
            if (admin.tableExists(table.getTableName())) {
                System.out.print("It does! Let's delete first");
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }

            System.out.println("Creating/recreating HBase table...");
            admin.createTable(table);
            System.out.println("Done!");
        }
    }

    private static MyNewsObject parseJson(String content) {
        JSONObject newsObject = new JSONObject(content);

        String id = newsObject.getString("id");
        String title = newsObject.getString("title");
        String text = newsObject.getString("text");

        return new MyNewsObject(id, title, text);
    }

    public static void streamBulkPut(JavaSparkContext jsc, String tableName, JavaDStream<MyNewsObject> putDStream) {
        Configuration conf = HBaseConfiguration.create();
//        conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
//        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
//        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "hdfs-site.xml"));

        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

        hbaseContext.streamBulkPut(putDStream, TableName.valueOf(tableName), new PutFunction());
    }

    public static class PutFunction implements Function<MyNewsObject, Put> {
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
