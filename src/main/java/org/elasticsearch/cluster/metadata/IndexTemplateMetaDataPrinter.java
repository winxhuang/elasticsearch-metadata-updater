package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class IndexTemplateMetaDataPrinter {

    public static XContentBuilder toInnerXContentWithTypes(
            IndexTemplateMetaData indexTemplateMetaData,
            XContentBuilder builder,
            ToXContent.Params params) throws IOException {
        IndexTemplateMetaData.Builder.toInnerXContentWithTypes(indexTemplateMetaData, builder, params);
        return builder;
    }
}
