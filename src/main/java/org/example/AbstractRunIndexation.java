package org.example;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.example.Main.BATCH_SIZE;
import static org.example.Main.SOLR_BASE_URL;
import static org.example.Main.SOLR_CORE;
import static org.example.Main.UPDATE_THREADS_NUMBER;

public abstract class AbstractRunIndexation implements RunIndexation
{
    @Override
    public void runIndexation() throws Exception
    {
        try (
            final ConcurrentUpdateSolrClient concurrentClient
                = new ConcurrentUpdateSolrClient.Builder(SOLR_BASE_URL)
                .withQueueSize(BATCH_SIZE)
                .withThreadCount(UPDATE_THREADS_NUMBER)
                .build())
        {

            // ensure it doesn't block where there's nothing to do yet
            concurrentClient.blockUntilFinished();

            // Delete all existing documents.
            concurrentClient.deleteByQuery(SOLR_CORE, "*:*");
            // commit is necessary because segment won't be removed and the new searcher won't be opened
            concurrentClient.commit(SOLR_CORE);

            long startMs = System.currentTimeMillis();
            final Map<String, String> stringStringMap = new LinkedHashMap<>(addDocuments(concurrentClient));

            // manually commit all documents
            concurrentClient.commit(SOLR_CORE);

            // wait until all requests are processed by cuss
            concurrentClient.blockUntilFinished();
            concurrentClient.shutdownNow();

            if (stringStringMap != null)
            {
                stringStringMap.put("Operation took", Math.round(((System.currentTimeMillis() - startMs) / 1000f) * 100f) / 100f + " sec");
                stringStringMap.entrySet().forEach(System.out::println);
            }
        }
    }

    protected abstract Map<String, String> addDocuments(final ConcurrentUpdateSolrClient cuss) throws Exception;
}
