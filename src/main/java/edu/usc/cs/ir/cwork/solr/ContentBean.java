package edu.usc.cs.ir.cwork.solr;

import org.apache.solr.client.solrj.beans.Field;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.Map;
import java.util.Set;

/**
 * Created by tg on 10/25/15.
 */
public class ContentBean {


    @Field("id")
    private String url;

    @Field
    private String content;

    @Field
    private String contentType;

    @Field
    private String mainType;

    @Field
    private String subType;

    @Field("*_md")
    private Map<String, Object> metadata;

    @Field
    private Set<String> persons;

    @Field
    private Set<String> organizations;

    @Field
    private Set<String> locations;

    @Field
    private Set<Date> dates;

    @Field
    private String host;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
        try {
            this.host = new URL(url).getHost();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
        if (contentType.contains("/")){
            String[] split = contentType.split("/");
            this.mainType = split[0];
            this.subType = split[1];
        }
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public String getHost() {
        return host;
    }

    public String getMainType() {
        return mainType;
    }

    public String getSubType() {
        return subType;
    }


    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }


    public Set<String> getPersons() {
        return persons;
    }

    public void setPersons(Set<String> persons) {
        this.persons = persons;
    }

    public Set<String> getOrganizations() {
        return organizations;
    }

    public void setOrganizations(Set<String> organizations) {
        this.organizations = organizations;
    }

    public Set<String> getLocations() {
        return locations;
    }

    public void setLocations(Set<String> locations) {
        this.locations = locations;
    }

    public Set<Date> getDates() {
        return dates;
    }

    public void setDates(Set<Date> dates) {
        this.dates = dates;
    }
}
