package fr.bcoquard.camelmysqlcdc.component;

public abstract class AMessage {
    private EMessageStructureType EMessageStructureType;

    public AMessage(EMessageStructureType EMessageStructureType) {
        this.EMessageStructureType = EMessageStructureType;
    }
}
