package miu.bdt;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MySocketServer {
    public static void main(String[] args) throws InterruptedException {

        int portNumber = 9999;

        try (
                ServerSocket serverSocket = new ServerSocket(portNumber);
                Socket clientSocket = serverSocket.accept();
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        ) {
            sendNewsToClient(out);
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port "
                    + portNumber + " or listening for a connection");
            System.out.println(e.getMessage());
        }
    }

    private static void sendNewsToClient(PrintWriter out) throws IOException, InterruptedException {
        String jsonContent = new String(Files.readAllBytes(Paths.get("news")));

        JSONArray jsonArray = new JSONArray(jsonContent);

        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String jsonObjectAsString = jsonObject.toString();
            out.println(jsonObjectAsString);
            Thread.sleep(2000);
        }
//            String inputLine;
//            while ((inputLine = stdIn.readLine()) != null) {
//                out.println(inputLine);
//            }
    }
}
