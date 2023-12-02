package org.example;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.noggit.ObjectBuilder;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.example.Main.SOLR_CORE;

public class IndexMovies extends AbstractRunIndexation
{
    private static final double DIVIDE_NUMBER = 5.5d;

    @Override
    protected Map<String, String> addDocuments(final ConcurrentUpdateSolrClient cuss) throws Exception
    {
        final Map<String, String> infoMap = new HashMap<>();
        long indexedDocuments = 0L;

        Set<Object> s = new HashSet<>();

        final List<Map<String, ?>> listOfData =
            (List<Map<String, ?>>) ObjectBuilder.fromJSON(new String(IndexUfoSightings.class.getResourceAsStream("/movies.json").readAllBytes(), StandardCharsets.UTF_8));

        for (final Map<String, ?> data : listOfData)
        {
            indexedDocuments++;
            final SolrInputDocument doc = new SolrInputDocument();
            doc.setField("id", computeDocId(data));
            doc.setField("title", data.get("title"));
            doc.setField("year", data.get("year"));
            doc.setField("cast", data.get("cast"));
            doc.setField("genres", data.get("genres"));
            doc.setField("extract", data.get("extract"));
            // because we don't have rating in the source... try to compute it
            doc.setField("rating", computeRating(data, DIVIDE_NUMBER));
            cuss.add(SOLR_CORE, doc);
            if (indexedDocuments % 1000 == 0)
            {
                System.out.printf("Processed %d documents.%n", indexedDocuments);
            }

        }

        infoMap.put("Movies indexing info", String.format("Sent %d documents", indexedDocuments));

        return infoMap;
    }

    private double computeRating(final Map<String, ?> data, final double divideNumber)
    {
        // should be stable from index to index
        double result = BigDecimal.valueOf(Math.abs(data.hashCode() / divideNumber) % 10).setScale(1, RoundingMode.HALF_UP).doubleValue();

        if (DIVIDE_NUMBER + 0.5d < divideNumber)
        {
            return 0.0d;
        }

        return result == 0.0d ? computeRating(data, divideNumber + 0.1d) : result;
    }

    private String computeDocId(final Map<String, ?> data)
    {
        // should be stable from index to index
        return String.format("%s/%s/%s",
                data.get("title") == null ? "" : data.get("title").toString().replaceAll("\\s*", ""),
                data.get("year"),
                DigestUtils.md5Hex(data.get("extract") + ""))
            .toLowerCase();

    }
}
