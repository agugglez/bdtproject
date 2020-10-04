package miu.bdt.spark;

import miu.bdt.sockets.SocketServerFile;
import miu.bdt.sockets.SocketServerKafka;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONObject;

public final class SparkStreamToConsole {
    private static final String HOSTNAME = "localhost";
    private static final int PORT = 9999;

    public static void main(String[] args) {
        // start socket server listening
        (new Thread(new SocketServerKafka(PORT))).start();

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaNetworkWordCount");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

        // Create a JavaReceiverInputDStream on target ip:port
        // in input stream of \n delimited text
        // Note that no duplication in storage level only for running locally.
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(HOSTNAME,
                PORT, StorageLevels.MEMORY_AND_DISK_SER);

        // parse the title from the news in JSON format
        JavaDStream<String> titles = lines.map(x -> parseTitle(x));

        titles.print();
        ssc.start();
        //ssc.awaitTermination();
    }

    private static String parseTitle(String newsJson){
        JSONObject newsObject = new JSONObject(newsJson);
        return newsObject.get("title").toString();
    }
}
