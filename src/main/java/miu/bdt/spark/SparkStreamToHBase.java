package miu.bdt.spark;

import miu.bdt.hbase.MyHBaseTable;
import miu.bdt.model.MyNewsObject;

import miu.bdt.sockets.SocketServerFile;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.json.JSONObject;

import java.io.IOException;

public class SparkStreamToHBase {

    private static final String HOSTNAME = "localhost";
    private static final int PORT = 9999;

    public static void main(String[] args) throws IOException {
        // start socket server listening
        (new Thread(new SocketServerFile(PORT))).start();

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

        // Create a JavaReceiverInputDStream on target ip:port
        // Note that no duplication in storage level only for running locally.
        // Replication necessary in distributed scenario for fault tolerance.
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(HOSTNAME,
                PORT, StorageLevels.MEMORY_AND_DISK_SER);

        JavaDStream<MyNewsObject> newsStream = lines.map(x -> parseJson(x))
                .filter(x -> x.getText().contains("big") && x.getText().contains("data"));

        newsStream.print();
        MyHBaseTable myHBaseTable = new MyHBaseTable("news");
        myHBaseTable.streamBulkPut(ssc.sparkContext(), newsStream);

        ssc.start();
        //ssc.awaitTermination();
    }

    private static MyNewsObject parseJson(String content) {
        JSONObject newsObject = new JSONObject(content);

        String id = newsObject.getString("id");
        String title = newsObject.getString("title");
        String text = newsObject.getString("text");

        return new MyNewsObject(id, title, text);
    }
}
