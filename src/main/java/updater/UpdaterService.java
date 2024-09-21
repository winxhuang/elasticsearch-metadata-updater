package updater;

import org.elasticsearch.Version;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.node.Node;
import org.elasticsearch.transport.TransportService;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.bootstrap.BootstrapWrap.definitelyRunningAsRoot;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_INDEX_VERSION_CREATED;
import static org.elasticsearch.node.InternalSettingsPreparer.prepareEnvironment;
import static updater.MetaDataPrinter.metaDataString;
import static updater.MetaDataPrinter.show;

@ShellComponent
public class UpdaterService {

    private Node node;

    @PostConstruct
    public void init() {
        // check if the user is running as root, and bail
        if (definitelyRunningAsRoot()) {
            throw new RuntimeException("can not run elasticsearch as root");
        }

        String esPathHome = System.getProperty("es.path.home");
        if (!StringUtils.hasText(esPathHome)) {
            throw new IllegalStateException("es.path.home is not configured");
        }

        // 创建environment
        final Map<String, String> settings = new HashMap<>();
        settings.put(Environment.PATH_HOME_SETTING.getKey(), esPathHome);
        settings.put("xpack.ml.enabled", "false");
        for (String key: System.getProperties().stringPropertyNames()) {
            if (key.startsWith("es.")) {
                settings.put(key.substring("es.".length()), System.getProperty(key));
            }
        }
        Environment environment = prepareEnvironment(Settings.EMPTY, settings, null, () -> "UpdaterNode");

        // 创建node
        node = new Node(environment);

        getInstance(TransportService.class).doStart();
        getInstance(GatewayMetaState.class).start(node);
    }

    private <T> T getInstance(Class<T> clazz) {
        return node.injector().getInstance(clazz);
    }

    @ShellMethod(key = "print node", value = "NodeMetaData, {path.data}/nodes/{id}/_state/node-x.st")
    public void printNode() {
        try {
            NodeMetaData nodeMetaData = node.getNodeEnvironment().getNodeMetaData();
            Terminal.DEFAULT.println(metaDataString(nodeMetaData));
        } catch (Exception e) {
            Terminal.DEFAULT.errorPrintln("print node error!");
            e.printStackTrace();
        }
    }

    @ShellMethod(key = "print manifest", value = "Manifest, {path.data}/nodes/{id}/_state/manifest-x.st")
    public void printManifest(@ShellOption(defaultValue = "10", value = "n", help = "foreach num") int num,
                              @ShellOption(defaultValue = "", value = "i", help = "index name") String name,
                              @ShellOption(defaultValue = "", value = "o", help = "output file path") String output) {
        try {
            Manifest manifest = getInstance(GatewayMetaState.class).getManifest();
            show(output, metaDataString(manifest, num, name));
        } catch (Exception e) {
            Terminal.DEFAULT.errorPrintln("print manifest error!");
            e.printStackTrace();
        }
    }

    @ShellMethod(key = "print global", value = "MetaData, {path.data}/nodes/{id}/_state/global-x.st")
    public void printGlobal(@ShellOption(defaultValue = "10", value = "n", help = "foreach num") int num,
                            @ShellOption(defaultValue = "512", value = "l", help = "foreach value limit len") int limit,
                            @ShellOption(defaultValue = "", value = "t", help = "template name") String tempName,
                            @ShellOption(defaultValue = "", value = "o", help = "output file path") String output) {
        try {
            MetaData metaData = getInstance(GatewayMetaState.class).getMetaData();
            show(output, metaDataString(metaData, num, limit, tempName));
        } catch (Exception e) {
            Terminal.DEFAULT.errorPrintln("print global error!");
            e.printStackTrace();
        }
    }

    @ShellMethod(key = "print index", value = "IndexMetaData, {path.data}/nodes/{id}/indices/{index_uuid}/_state/state-x.st")
    public void printIndex(@ShellOption(defaultValue = "", value = "i", help = "index name") String name,
                           @ShellOption(defaultValue = "", value = "o", help = "output file path") String output) {
        try {
            MetaData metaData = getInstance(GatewayMetaState.class).getMetaData();
            show(output, metaDataString(metaData, name));
        } catch (Exception e) {
            Terminal.DEFAULT.errorPrintln("print index error!");
            e.printStackTrace();
        }
    }

    @ShellMethod(key = "update version", value = "update metadata version, default update to 7.5.1")
    public void updateVersion(
            @ShellOption(defaultValue = "", value = "v", help = "version") String ver,
            @ShellOption(defaultValue = "all", value = "s", help = "scope [node|global|all]") String scope) {
        Version version;
        try {
            if (ver != null && ver.length() > 0 && Integer.parseInt(ver) > 0) {
                version = Version.fromId(Integer.parseInt(ver));
            } else {
                version = Version.V_7_5_1;
            }
            Terminal.DEFAULT.println(String.format("update to Version[%s/%s]", version, version.id));
        } catch (Exception e) {
            Terminal.DEFAULT.errorPrintln("write nodeMetaData error!");
            e.printStackTrace();
            return;
        }
        try {
            if (scope.equalsIgnoreCase("node") || scope.equalsIgnoreCase("all")) {
                Terminal.DEFAULT.println("update nodeMetaData...");
                NodeMetaData nodeMetaData = node.getNodeEnvironment().getNodeMetaData();
                nodeMetaData.setNodeVersion(version);
                NodeMetaData.FORMAT.writeAndCleanup(nodeMetaData, node.getNodeEnvironment().nodeDataPaths());
                Terminal.DEFAULT.println("update nodeMetaData done.");
            }
        } catch (Exception e) {
            Terminal.DEFAULT.errorPrintln("write nodeMetaData error!");
            e.printStackTrace();
            return;
        }
        try {
            if (scope.equalsIgnoreCase("global") || scope.equalsIgnoreCase("all")) {
                Terminal.DEFAULT.println("update clusterState...");
                GatewayMetaState metaState = getInstance(GatewayMetaState.class);
                ClusterState clusterState = metaState.getPersistedState().getLastAcceptedState();
                for (IndexMetaData indexMetaData : clusterState.getMetaData()) {
                    Version createdVersion = SETTING_INDEX_VERSION_CREATED.get(indexMetaData.getSettings());
                    if (createdVersion != null && createdVersion.id != version.id) {
                        Settings.Builder indexSettingsBuilder = Settings.builder();
                        indexSettingsBuilder.put(indexMetaData.getSettings());
                        indexSettingsBuilder.put(IndexMetaData.SETTING_INDEX_VERSION_CREATED.getKey(), version);
                        indexMetaData.setSettings(indexSettingsBuilder.build());
                    }
                }
                metaState.getPersistedState().setLastAcceptedState(clusterState);
                Terminal.DEFAULT.println("update clusterState done.");
            }
        } catch (Exception e) {
            Terminal.DEFAULT.errorPrintln("write nodeMetaData error!");
            e.printStackTrace();
        }
    }

    @ShellMethod(key = "reload metadata", value = "reload nodeMetaData/metaData/manifest")
    public void reloadMetaDate(
            @ShellOption(defaultValue = "all", value = "s", help = "scope [node|global|all]") String scope) {
        try {
            if (scope.equalsIgnoreCase("node") || scope.equalsIgnoreCase("all")) {
                node.getNodeEnvironment().reload(node.getEnvironment());
            }
        } catch (Exception e) {
            Terminal.DEFAULT.errorPrintln("reload nodeMetaData error!");
            e.printStackTrace();
            return;
        }
        try {
            if (scope.equalsIgnoreCase("global") || scope.equalsIgnoreCase("all")) {
                getInstance(GatewayMetaState.class).start(node);
            }
        } catch (Exception e) {
            Terminal.DEFAULT.errorPrintln("reload metaData/manifest error!");
            e.printStackTrace();
        }
    }
}
