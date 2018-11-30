package cleanup;

import java.util.HashMap;
import java.util.Map;

public class CarrierLookup {
    public static Map<String, String> carrierLookup = new HashMap<>();

    static {
        carrierLookup.put("9E", "1");
        carrierLookup.put("AA", "2");
        carrierLookup.put("AS", "3");
        carrierLookup.put("B6", "4");
        carrierLookup.put("DL", "5");
        carrierLookup.put("EV", "6");
        carrierLookup.put("F9", "7");
        carrierLookup.put("G4", "8");
        carrierLookup.put("HA", "9");
        carrierLookup.put("MQ", "10");
        carrierLookup.put("NK", "11");
        carrierLookup.put("OH", "12");
        carrierLookup.put("OO", "13");
        carrierLookup.put("UA", "14");
        carrierLookup.put("VX", "15");
        carrierLookup.put("WN", "16");
        carrierLookup.put("YV", "17");
        carrierLookup.put("YX", "18");
    }

    public static String getCarrier(String carrier) {
        return carrierLookup.getOrDefault(carrier, "0");
    }
}
