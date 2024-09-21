package org.elasticsearch.bootstrap;

public class BootstrapWrap {

    public static boolean definitelyRunningAsRoot() {
        return Natives.definitelyRunningAsRoot();
    }
}
