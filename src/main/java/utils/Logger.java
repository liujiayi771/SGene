package utils;

public class Logger {
    /*
     * levels of debugging
     * 0: INFO
     * 1: DEBUG
     * 2: EXCEPTION
     * 3:ALL
     */
    private static int LEVEL = 2;
    private static final int EXCEPTION = 2;
    private static final int DEBUG = 1;
    private static final int INFO = 0;
    // TODO use configuration set LEVEL
    public static void SETLEVEL(int NEWLEVEL) {
        LEVEL = NEWLEVEL;
    }
    public static void EXCEPTION(Exception ex) {
        if (LEVEL >= EXCEPTION) {
            System.err.println("[EXCEPTION] " + ex.getLocalizedMessage());
            ex.printStackTrace();
        }
    }
    public static void DEBUG(String message) {
        if (LEVEL >= DEBUG)
            System.err.println("[" + Timer.getGlobalTime() + " - DEBUG] " + message);
    }
    public static void INFO(String message) {
        if (LEVEL >= INFO)
            System.err.println("[INFO] " + message);
    }
    public static void INFOTIME(String message) {
        if (LEVEL >= INFO)
            System.out.println(Timer.getGlobalTime() + " INFO " + message);
    }
    public static void DEBUG(String message, int level) {
        if (LEVEL >= level)
            System.err.println("[" + Timer.getGlobalTime() + " - DEBUG] " + message);
    }
    public static void PROFILE(String message) {
        System.err.println("[PROFILE] [" + Timer.getGlobalTime() + "] " + message);
    }
}
