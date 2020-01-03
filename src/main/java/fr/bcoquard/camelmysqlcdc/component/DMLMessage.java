package fr.bcoquard.camelmysqlcdc.component;

import java.util.HashMap;
import java.util.Map;

public class DMLMessage extends AMessage {

    private EMessageOperationType EMessageOperationType;
    private Map<PayloadType, MessagePayload> payloads;

    public DMLMessage(EMessageOperationType EMessageOperationType) {
        super(EMessageStructureType.DML);

        this.payloads = new HashMap<>();
        this.payloads.put(PayloadType.OLD, null);
        this.payloads.put(PayloadType.NEW, null);

        this.EMessageOperationType = EMessageOperationType;
    }

    public boolean addPayload(PayloadType payloadType, MessagePayload payload) {
        return this.payloads.put(payloadType, payload) != null;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{");
        if (payloads.get(PayloadType.OLD) == null) {
            stringBuilder.append("old= null");
        } else {
            stringBuilder.append("old= ").append(payloads.get(PayloadType.OLD).toString());
        }
        stringBuilder.append(" | ");
        if (payloads.get(PayloadType.NEW) == null) {
            stringBuilder.append("new= null");
        } else {
            stringBuilder.append("new= ").append(payloads.get(PayloadType.NEW).toString());
        }
        stringBuilder.append("}");

        return stringBuilder.toString();
    }
}
