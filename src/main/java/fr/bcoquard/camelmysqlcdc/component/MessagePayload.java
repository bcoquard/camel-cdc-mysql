package fr.bcoquard.camelmysqlcdc.component;

import java.util.HashMap;
import java.util.Map;

public class MessagePayload {
    private Map<String, Object> payload;

    public MessagePayload() {
        this.payload = new HashMap<>();
    }

    public boolean addOne(String keyCol, Object valueCol) {
        return payload.put(keyCol, valueCol) != null;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry iter : payload.entrySet()) {
            if (stringBuilder.length() != 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(iter.getKey() + ": " + iter.getValue());
        }
        return stringBuilder.toString();
    }
}
