package updater;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.metadata.*;
import org.elasticsearch.cluster.metadata.MetaData.Custom;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeMetaData;
import org.elasticsearch.index.Index;
import org.elasticsearch.transport.TransportService;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class MetaDataPrinter {

    private static final Logger logger = LogManager.getLogger(MetaDataPrinter.class);

    private static final String TAB2 = "  ";
    private static final String TAB3 = "   ";

    public static String metaDataString(NodeMetaData nodeMetaData) {
        StringBuilder sb = new StringBuilder();
        sb.append("NodeMetaData:\n");
        sb.append(TAB2).append("nodeId: ").append(nodeMetaData.nodeId()).append("\n");
        sb.append(TAB2).append("nodeVersion: ").append(nodeMetaData.nodeVersion()).append("\n");
        return sb.toString();
    }

    public static String metaDataString(Manifest manifest, int num, String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("Manifest:\n");
        sb.append(TAB2).append("currentTerm: ").append(manifest.getCurrentTerm()).append("\n");
        sb.append(TAB2).append("clusterStateVersion: ").append(manifest.getClusterStateVersion()).append("\n");
        sb.append(TAB2).append("globalGeneration: ").append(manifest.getGlobalGeneration()).append("\n");
        sb.append(TAB2).append(String.format("indexGenerations: (%s)", manifest.getIndexGenerations().size())).append("\n");
        if (name != null && name.length() > 0) {
            manifest.getIndexGenerations().forEach((k, v) -> {
                if (k.getName().equalsIgnoreCase(name) || k.getUUID().equalsIgnoreCase(name)) {
                    sb.append(TAB2).append(TAB2).append(k).append(", ").append("generation: ").append(v).append("\n");
                }
            });
        } else {
            int count = 0;
            for (Map.Entry<Index, Long> index : manifest.getIndexGenerations().entrySet()) {
                if (num >= 0 && (++count) > num) {
                    sb.append(TAB2).append(TAB2).append("...\n");
                    break;
                }
                sb.append(TAB2).append(TAB2)
                        .append(index.getKey().toString()).append(", ")
                        .append("generation: ").append(index.getValue()).append("\n");
            }
        }
        return sb.toString();
    }

    public static String metaDataString(MetaData metaData, int num, int limit, String tempName) {
        StringBuilder sb = new StringBuilder();
        sb.append("MetaData:\n");
        sb.append(TAB2).append("cluster_uuid: ").append(metaData.clusterUUID())
                .append(" [committed: ").append(metaData.clusterUUIDCommitted()).append("]").append("\n");
        sb.append(TAB2).append("version: ").append(metaData.version()).append("\n");

        CoordinationMetaData coordinationMetaData = metaData.coordinationMetaData();
        sb.append(TAB2).append("coordination_metadata:\n");
        sb.append(TAB2).append(TAB3).append("term: ").append(coordinationMetaData.term()).append("\n");
        sb.append(TAB2).append(TAB3).append("last_committed_config: ").append(coordinationMetaData.getLastCommittedConfiguration()).append("\n");
        sb.append(TAB2).append(TAB3).append("last_accepted_config: ").append(coordinationMetaData.getLastAcceptedConfiguration()).append("\n");
        sb.append(TAB2).append(TAB3).append("voting_config_exclusions: ").append(coordinationMetaData.getVotingConfigExclusions()).append("\n");

        sb.append(TAB2).append(String.format("persistent_settings: (%s)\n", metaData.persistentSettings().size()));
        if (!metaData.persistentSettings().isEmpty()) {
            sb.append(TAB2).append(TAB3).append(metaData.persistentSettings().toString()).append("\n");
        }
        sb.append(TAB2).append(String.format("transient_settings: (%s)\n", metaData.transientSettings().size()));
        if (!metaData.transientSettings().isEmpty()) {
            sb.append(TAB2).append(TAB3).append(metaData.transientSettings().toString()).append("\n");
        }
        sb.append(TAB2).append(String.format("hashes_of_consistent_settings: (%s)\n", metaData.hashesOfConsistentSettings().size()));
        if (!metaData.hashesOfConsistentSettings().isEmpty()) {
            sb.append(TAB2).append(TAB3).append(metaData.hashesOfConsistentSettings().toString()).append("\n");
        }

        sb.append(TAB2).append(String.format("templates: (%s)\n", metaData.templates().size()));
        int count = 0;
        for (ObjectCursor<IndexTemplateMetaData> cursor : metaData.templates().values()) {
            try {
                if (tempName != null && tempName.length() > 0) {
                    if (cursor.value.getName().equalsIgnoreCase(tempName)) {
                        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
                        IndexTemplateMetaDataPrinter.toInnerXContentWithTypes(
                                cursor.value, xContentBuilder, ToXContent.EMPTY_PARAMS).endObject().flush();
                        sb.append(TAB2).append(TAB3)
                                .append(cursor.value.getName()).append(": ")
                                .append(xContentBuilder.getOutputStream().toString()).append("\n");
                    }
                    continue;
                }
                if (num >= 0 && ++count > num) {
                    sb.append(TAB2).append(TAB3).append("...\n");
                    break;
                }
                XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
                IndexTemplateMetaDataPrinter.toInnerXContentWithTypes(
                        cursor.value, xContentBuilder, ToXContent.EMPTY_PARAMS).endObject().flush();
                sb.append(TAB2).append(TAB3)
                        .append(cursor.value.getName()).append(": ")
                        .append(limitStr(xContentBuilder.getOutputStream().toString(), limit)).append("\n");
            } catch (Exception e) {
                logger.error("parse templates[{}] error: {}", cursor.value.getName(), e.toString());
            }
        }

        sb.append(TAB2).append(String.format("indices: (%s)\n", metaData.getIndices().size()));
        count = 0;
        for (IndexMetaData indexMetaData : metaData) {
            try {
                if (num >= 0 && (++count) > num) {
                    sb.append(TAB2).append(TAB3).append("...\n");
                    break;
                }
                XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
                indexMetaData.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS).endObject().flush();
                sb.append(TAB2).append(TAB3)
                        .append(indexMetaData.getIndex()).append(": ")
                        .append(limitStr(xContentBuilder.getOutputStream().toString(), limit)).append("\n");
            } catch (Exception e) {
                logger.error("parse index[{}] error: {}", indexMetaData.getIndex().getName(), e.toString());
            }
        }

        sb.append(TAB2).append(String.format("customs: (%s)\n", metaData.customs().size()));
        if (!metaData.customs().isEmpty()) {
            for (final ObjectObjectCursor<String, Custom> cursor : metaData.customs()) {
                final String type = cursor.key;
                final MetaData.Custom custom = cursor.value;
                try {
                    XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
                    custom.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS).endObject().flush();
                    sb.append(TAB2).append(TAB3)
                            .append(type).append(": ")
                            .append(limitStr(xContentBuilder.getOutputStream().toString(), limit)).append("\n");
                } catch (Exception e) {
                    logger.error("parse customs[{}] error: {}", type, e.toString());
                }
            }
        }
        return sb.toString();
    }

    public static String metaDataString(MetaData metaData, String name) {
        StringBuilder sb = new StringBuilder();
        sb.append("IndexMetaData:\n");
        if (name != null && name.length() > 0) {
            for (IndexMetaData indexMetaData : metaData) {
                try {
                    if (indexMetaData.getIndex().getName().equalsIgnoreCase(name)
                        || indexMetaData.getIndex().getUUID().equalsIgnoreCase(name)) {
                        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON).startObject();
                        indexMetaData.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS).endObject().flush();
                        sb.append(TAB2).append(TAB3)
                                .append(indexMetaData.getIndex()).append(": ")
                                .append(xContentBuilder.getOutputStream()).append("\n");
                    }
                } catch (Exception e) {
                    logger.error("parse index[{}] error: {}", name, e.toString());
                }
            }
        } else {
            sb.append(TAB2).append(TAB3).append("not found");
        }
        return sb.toString();
    }

    private static String limitStr(String str, int limit) {
        if (str != null && limit >= 0 && str.length() > limit) {
            return str.substring(0, limit);
        }
        return str;
    }

    public static void show(String filePath, String content) {
        if (filePath != null && filePath.length() > 0) {
            toFile(filePath, content);
        } else {
            Terminal.DEFAULT.println(content);
        }
    }

    public static void toFile(String filePath, String content) {
        if (filePath.endsWith(File.separator)) {
            Terminal.DEFAULT.errorPrintln(String.format("%s is not a file path!", filePath));
            return;
        }
        int idx = filePath.lastIndexOf(File.separator);
        if (idx >= 0) {
            String dir = filePath.substring(0, idx);
            try {
                Files.createDirectories(Paths.get(dir));
            } catch (Exception e) {
                Terminal.DEFAULT.errorPrintln(String.format("create directory[%s] error!", dir));
                e.printStackTrace();
                return;
            }
        }
        boolean fileOk = true;
        File output = new File(filePath);
        if (!output.exists()) {
            try {
                fileOk = output.createNewFile();
            } catch (Exception e) {
                Terminal.DEFAULT.errorPrintln(String.format("create file[%s] error!", filePath));
                e.printStackTrace();
                return;
            }
        }
        if (fileOk) {
            try (PrintStream ps = new PrintStream(output)) {
                ps.println(content);
            } catch (Exception e) {
                Terminal.DEFAULT.errorPrintln(String.format("write file[%s] error!", filePath));
                e.printStackTrace();
            }
        }
    }
}
