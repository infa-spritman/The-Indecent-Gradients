package AttributeAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class ExpectedCount {
    public static void main(String[] args) throws IOException {
        long totalEntries = Long.parseLong(args[1]);
        int targetIndex = Integer.parseInt(args[2]);

        HashMap<Integer, HashMap<String, Integer>> mapping = new HashMap<>();

        BufferedReader br = new BufferedReader(new FileReader(args[0]));
        String line = br.readLine();
    }
}
