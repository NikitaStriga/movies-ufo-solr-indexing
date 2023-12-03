package org.example;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Main
{
    public static final String SOLR_BASE_URL = System.getProperty("solr.base.url") == null ? "http://localhost:8983/solr/"
                                                                                            : (System.getProperty("solr.base.url").endsWith("/")
                                                                                               ? System.getProperty("solr.base.url")
                                                                                               :
                                                                                               System.getProperty("solr.base.url") + "/");

    public static final int BATCH_SIZE = Optional.ofNullable(System.getProperty("batch.size")).map(Integer::parseInt).orElse(500);
    public static final int UPDATE_THREADS_NUMBER = Optional.ofNullable(System.getProperty("update.threads.number")).map(Integer::parseInt).orElse(1);

    public static final boolean BE_VERBOSE = Boolean.parseBoolean(System.getProperty("be.verbose"));

    public static final String DATA_SET = System.getProperty("data.set");
    private static final String UFO_DATA_SET = "ufo";
    private static final String MOVIES_DATA_SET = "movies";
    private static final Set<String> AVAILABLE_DATA_SETS = Set.of(UFO_DATA_SET, MOVIES_DATA_SET);

    private static final Map<String, RunIndexation> INDEXERS = Map.of(UFO_DATA_SET, new IndexUfoSightings(), MOVIES_DATA_SET, new IndexMovies());

    public static final String SOLR_CORE = Optional.ofNullable(System.getProperty("solr.core")).orElse(System.getProperty("data.set"));

    public static void main(final String[] args)
    {
        boolean validConfig = true;

        if (DATA_SET == null || !AVAILABLE_DATA_SETS.contains(DATA_SET))
        {
            if (DATA_SET == null)
            {
                System.err.println("Missing -Ddata.set system property. Available values: " + AVAILABLE_DATA_SETS);
            }
            else
            {
                System.err.println("-Ddata.set system property has wrong value. Available values: " + AVAILABLE_DATA_SETS);
            }

            validConfig = false;
        }

        if (!validConfig)
        {
            return;
        }

        printInfo();

        // run indexation
        try
        {
            INDEXERS.get(DATA_SET).runIndexation();
        }
        catch (final Exception exception)
        {
            System.err.println("Exception has occurred while indexing data:\n" + exception + "\n" + ExceptionUtils.getStackTrace(exception));
        }

    }

    private static void printInfo()
    {
        System.out.println("CONFIGURATION:");
        final Map<String, Object> info = new LinkedHashMap<>();
        info.put("  -   SOLR_BASE_URL (-Dsolr.base.url)", SOLR_BASE_URL);
        info.put("  -   BATCH_SIZE (-Dbatch.size)", BATCH_SIZE);
        info.put("  -   UPDATE_THREADS_NUMBER (-Dupdate.threads.number)", UPDATE_THREADS_NUMBER);
        info.put("  -   DATA_SET (-Ddata.set)", DATA_SET);
        info.put("  -   SOLR_CORE (-Dsolr.core)", SOLR_CORE);
        info.put("  -   BE_VERBOSE (-Dbe.verbose)", BE_VERBOSE);
        info.entrySet().forEach(System.out::println);
        System.out.println("*******************************************");
    }

}
