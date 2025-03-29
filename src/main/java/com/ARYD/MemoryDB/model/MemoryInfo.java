package com.ARYD.MemoryDB.model;

public class MemoryInfo {
    public static void main(String[] args) {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long allocatedMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();

        System.out.println("Mémoire maximale (Xmx) : " + (maxMemory / 1024 / 1024) + " MB");
        System.out.println("Mémoire allouée (totalMemory) : " + (allocatedMemory / 1024 / 1024) + " MB");
        System.out.println("Mémoire libre : " + (freeMemory / 1024 / 1024) + " MB");
    }
}
