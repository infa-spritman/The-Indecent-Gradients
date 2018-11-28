package AttributeAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class ExpectedCount {

    public static void main(String[] args) throws IOException {
        long totalEntries = Long.parseLong(args[1]);
        int targetIndex = Integer.parseInt(args[2]);

        HashMap<Integer, HashMap<String, Integer>> mapping = new HashMap<>();

        BufferedReader br = new BufferedReader(new FileReader(args[0]));
        String line = br.readLine();
        while(line != null){
            String[] lineSplit = line.split(",");
            Integer index = Integer.parseInt(lineSplit[0]);
            HashMap<String, Integer> indexMap = new HashMap<>();

            if(mapping.containsKey(index)){
                indexMap = mapping.get(index);
            }
            else{
                mapping.put(index, indexMap);
            }
            indexMap.put(lineSplit[1], Integer.parseInt(lineSplit[2]));
            line = br.readLine();
        }

        HashMap<String, Integer> targetMap = mapping.get(targetIndex);
        PrintWriter writer = new PrintWriter("the-file-name.txt", "UTF-8");

        for(Map.Entry<Integer, HashMap<String, Integer>> entry : mapping.entrySet()){
            if(entry.getKey() != targetIndex){
                for(Map.Entry<String, Integer> innerEntry : entry.getValue().entrySet()){
                    for(Map.Entry<String, Integer> targetEntry : targetMap.entrySet()){
                        Long value = (long)innerEntry.getValue() * (long)targetEntry.getValue()/totalEntries;
                        String newEntry = entry.getKey() + "," + innerEntry.getKey() + "," + targetEntry.getKey() + "," + value;
                        writer.println(newEntry);
                    }
                }
            }
        }
        writer.close();
    }
}
