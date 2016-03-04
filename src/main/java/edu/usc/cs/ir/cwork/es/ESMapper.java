package edu.usc.cs.ir.cwork.es;

import edu.usc.cs.ir.cwork.solr.ContentBean;
import org.json.JSONObject;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by tg on 3/3/16.
 */
public class ESMapper  {

    public static Map<String, Object> toCDRSchema(ContentBean contentBean) {
        Map<String, Object> doc = new LinkedHashMap<>();

        String id = contentBean.getId();
        doc.put("obj_id", id);
        /*FIXME: The below info not available
        if (datum.getMetaData().containsKey("obj_parent")) {
            doc.add("obj_parent", datum.getMetaData().get("obj_parent").toString());
        } else {
            if (inlinks != null && inlinks.size() > 0) {
                doc.add("obj_parent", DigestUtils.sha256Hex(inlinks.iterator().next().getFromUrl().toUpperCase()));
            } else {
                // do nothing
            }
        }
        */
        doc.put("obj_children", contentBean.getOutpaths());
        doc.put("obj_childrenurls", contentBean.getOutlinks());

        doc.put("content_type", contentBean.getContentType());
        //doc.add("crawl_data", extractedData);
        doc.put("crawler", "Nutch-1.12-SNAPSHOT");

        JSONObject extractedMd = new JSONObject(contentBean.getMetadata());
        doc.put("extracted_metadata", extractedMd);
        doc.put("extracted_text", contentBean.getContent());
        doc.put("obj_original_url", contentBean.getUrl());
        String storedUrl = id.replace(
                "file:/data2/USCWeaponsStatsGathering/nutch/full_dump/",
                "http://imagecat.dyndns.org/weapons/alldata/");
        doc.put("obj_stored_url", storedUrl);

        doc.put("team", "NASA_JPL");
        //FIXME: The below info not available
        // doc.add("timestamp", datum.getFetchTime());
        doc.put("url", contentBean.getUrl());
        doc.put("version", (float)2.0);

        doc.put("raw_content", contentBean.getRawContent());
        return doc;
    }

}
