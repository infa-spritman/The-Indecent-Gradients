package AttributeAnalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class ChiSquared {
    public static void main(String[] args) throws IOException {
        HashMap<Integer, HashMap<String, HashMap<Integer, Double>>> expected = readFile(args[0]);
        HashMap<Integer, HashMap<String, HashMap<Integer, Double>>> observed = readFile(args[1]);
        completeTheHashMap(observed);

        Double[] chiValues = new Double[expected.size() + 1];

        for(Map.Entry<Integer, HashMap<String, HashMap<Integer, Double>>> entry0 : expected.entrySet()){
            double chiValue = 0.0;
            HashMap<String, HashMap<Integer, Double>> observedEntry0 = observed.get(entry0.getKey());
            for(Map.Entry<String, HashMap<Integer, Double>> entry1 : entry0.getValue().entrySet()){
                HashMap<Integer, Double> observedEntry1 = observedEntry0.get(entry1.getKey());

                for(Map.Entry<Integer, Double> entry2 : entry1.getValue().entrySet()){
                    Double observedEntry2 = observedEntry1.get(entry2.getKey());
                    if(entry2.getValue() != 0)
                        chiValue += Math.pow((observedEntry2 - entry2.getValue()),2)/entry2.getValue();
                }
            }
            chiValues[entry0.getKey()] = chiValue;
        }

        PrintWriter write = new PrintWriter("chi.txt", "UTF-8");

        for(int i = 1; i < chiValues.length; i++){
            write.println(i + "," + chiValues[i-1]);
        }
        write.close();
    }

    // Makes the observed hashmap consistent by adding cells having 0 value
    public static void completeTheHashMap(HashMap<Integer, HashMap<String, HashMap<Integer, Double>>> map){
        for(Map.Entry<Integer, HashMap<String, HashMap<Integer, Double>>> entry0: map.entrySet()){
            for(Map.Entry<String, HashMap<Integer, Double>> entry1 : entry0.getValue().entrySet()){
                for(int i = 0; i < 14; i++){
                    if(!entry1.getValue().containsKey(i)){
                        entry1.getValue().put(i, 0.0);
                    }
                }
            }
        }
    }


    // Reads the observed frequency output and expected frequency output file
    public static HashMap<Integer, HashMap<String, HashMap<Integer, Double>>> readFile(String fileAddress) throws IOException {
        HashMap<Integer, HashMap<String, HashMap<Integer, Double>>> ans = new HashMap<>();

        BufferedReader br = new BufferedReader(new FileReader(fileAddress));
        String line = br.readLine();
        while(line != null){
            String[] splitLine = line.split(",");
            int index = Integer.parseInt(splitLine[0]);
            HashMap<String, HashMap<Integer, Double>> columnMap = new HashMap<>();
            if(ans.containsKey(index)){
                columnMap = ans.get(index);
            }
            else{
                ans.put(index, columnMap);
            }
            HashMap<Integer, Double> domainMap = new HashMap<>();
            if(columnMap.containsKey(splitLine[1])){
                domainMap = columnMap.get(splitLine[1]);
            }
            else{
                columnMap.put(splitLine[1], domainMap);
            }

            domainMap.put(Integer.parseInt(splitLine[2]), Double.parseDouble(splitLine[3]));

            line = br.readLine();
        }
        return ans;
    }
}
