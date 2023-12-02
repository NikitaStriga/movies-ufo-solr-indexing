package org.example;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.noggit.ObjectBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.example.Main.BATCH_SIZE;
import static org.example.Main.BE_VERBOSE;
import static org.example.Main.SOLR_BASE_URL;
import static org.example.Main.SOLR_CORE;
import static org.example.Main.UPDATE_THREADS_NUMBER;

public class IndexUfoSightings extends AbstractRunIndexation
{
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat MONTH_NAME_FMT = new SimpleDateFormat("MMMM");

    private static final Pattern MATCH_US_CITY_AND_STATE = Pattern.compile("^([^,]+),\\s([A-Z]{2})$");


    protected Map<String, String> addDocuments(final ConcurrentUpdateSolrClient concurrentClient) throws Exception
    {
        final Map<String, String> infoMap = new HashMap<>();

        long startMs = System.currentTimeMillis();
        int numSent = 0;
        int numSkipped = 0;
        int lineNum = 0;
        SolrInputDocument doc = null;
        String line = null;

        // read file line-by-line
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(
            // in /ufo_awesome.json / is mandatory!
            IndexUfoSightings.class.getResourceAsStream("/ufo_awesome.json"))))
        {
            // process each sighting as a document
            while ((line = reader.readLine()) != null)
            {
                doc = parseNextDoc(line, ++lineNum);
                if (doc != null)
                {
                    concurrentClient.add(SOLR_CORE, doc);
                    ++numSent;
                }
                else
                {
                    ++numSkipped;
                    continue;
                }

                if (lineNum % 1000 == 0)
                {
                    System.out.printf("Processed %d documents.%n", lineNum);
                }
            }
        }

        infoMap.put("Ufo indexing info", String.format("Sent %d documents (skipped %d)", numSent, numSkipped));

        return infoMap;
    }

    protected SolrInputDocument parseNextDoc(String line, int lineNum)
    {
        Map jsonObj = null;
        try
        {
            jsonObj = (Map) ObjectBuilder.fromJSON(line);
        }
        catch (Exception jsonErr)
        {
            if (BE_VERBOSE)
            {
                System.out.println("Skipped invalid sighting at line " + lineNum + "; Failed to parse [" + line + "] into JSON due to: " + jsonErr);
            }
            return null;
        }

        String sighted_at = readField(jsonObj, "sighted_at");
        String location = readField(jsonObj, "location");
        String description = readField(jsonObj, "description");

        // ignore rows that don't have valid data
        if (sighted_at == null || location == null || description == null)
        {
            if (BE_VERBOSE)
            {
                System.out.println("Skipped incomplete sighting at line " + lineNum + "; " + line);
            }
            return null;
        }

        // require the sighted_at date to be valid
        Date sighted_at_dt = null;
        try
        {
            sighted_at_dt = DATE_FORMATTER.parse(sighted_at);
        }
        catch (java.text.ParseException pe)
        {
            if (BE_VERBOSE)
            {
                System.out.println("Skipped sighting at line " + lineNum + " due to invalid sighted_at date (" + sighted_at + ") caused by: " + pe);
            }
            return null;
        }

        // Verify the location matches the pattern of US City and State
        Matcher matcher = MATCH_US_CITY_AND_STATE.matcher(location);
        if (!matcher.matches())
        {
            if (BE_VERBOSE)
            {
                System.out.println("Skipped sighting at line " + lineNum + " because location [" + location + "] does not look like a US city and state.");
            }
            return null;
        }

        // split the cit and state into separate fields
        String city = matcher.group(1);
        String state = matcher.group(2);

        // Clean-up the sighting description, mostly for display purposes

        // description has some XML escape sequences ... convert back to chars
        description = description.replace("&quot;", "\"").replace("&amp;", "&").replace("&apos;", "'");
        description = description.replaceAll("\\s+", " "); // collapse all whitespace down to 1 space
        description = description.replaceAll("([a-z])([\\.\\?!,;])([A-Z])", "$1$2 $3"); // fix missing space at end of sentence
        description = description.replaceAll("([a-z])([A-Z])", "$1 $2"); // fix missing space between end of word and new word

        String reported_at = readField(jsonObj, "reported_at");
        String shape = readField(jsonObj, "shape");
        String duration = readField(jsonObj, "duration");

        // every doc needs a unique id - create a composite key based on sighting data
        String docId = String.format("%s/%s/%s/%s/%s/%s",
                sighted_at,
                (reported_at != null ? reported_at : "?"),
                city.replaceAll("\\s+", ""),
                state,
                (shape != null ? shape : "?"),
                DigestUtils.md5Hex(description))
            .toLowerCase();

        SolrInputDocument doc = new SolrInputDocument();
        doc.setField("id", docId);
        doc.setField("sighted_at_dt", sighted_at_dt);
        // another field to facet on
        doc.setField("month_s", MONTH_NAME_FMT.format(sighted_at_dt));

        if (reported_at != null)
        {
            try
            {
                doc.setField("reported_at_dt", DATE_FORMATTER.parse(reported_at));
            }
            catch (java.text.ParseException pe)
            {
                // not fatal - just ignore this invalid field
            }
        }

        doc.setField("city_s", city);
        doc.setField("state_s", state);
        doc.setField("location_s", location); // keep this field around for faceting on the full location

        if (shape != null)
        {
            doc.setField("shape_s", shape);
        }

        if (duration != null)
        {
            doc.setField("duration_s", duration);
        }

        doc.setField("sighting_en", description);

        return doc;
    }

    protected String readField(Map jsonObj, String key)
    {
        String val = (String) jsonObj.get(key);
        if (val != null)
        {
            val = val.trim();
            if (val.length() == 0)
            {
                val = null;
            }
        }
        return val;
    }
}
