select p.paperid, p.doi, p.papertitle, p.year, p.citationcount,
       ci.normalizedname as venuename,
       ptopics.topics, pref.references, purl.urls,
       pdetails.authors, 
       abstr.abstract
from azure_mag.paper as p
    left outer join azure_mag.paperabstract as abstr
        on p.paperid = abstr.paperid

    left outer join azure_mag.conferenceinstance as ci
        on p.conferenceinstanceid = ci.conferenceinstanceid

    left outer join (select paperid, collect_set(paperreferenceid) as references
                     from azure_mag.paperreference
                     group by paperid) as pref
        on p.paperid = pref.paperid
    
    left outer join (select paperid, collect_set(sourceurl) as urls
                     from azure_mag.paperurl
                     group by paperid) as purl
        on p.paperid = purl.paperid
    
    left outer join (select paperfieldofstudy.paperid, collect_set(fieldsofstudy.normalizedname) as topics
                     from azure_mag.paperfieldofstudy
                        left outer join azure_mag.fieldsofstudy
                            on paperfieldofstudy.fieldofstudyid = fieldsofstudy.fieldofstudyid
                     group by paperfieldofstudy.paperid) as ptopics
        on p.paperid = ptopics.paperid

    left outer join (select paperauthoraffiliation.paperid, collect_set(named_struct('order', paperauthoraffiliation.authorsequencenumber,
                                                                                    'authorid', author.authorid,
                                                                                    'name', author.normalizedname, 
                                                                                    'gridid', gridinfo.gridid,
                                                                                    'affiliation', gridinfo.name,
                                                                                    'country', gridinfo.country)) as authors
                     from azure_mag.paperauthoraffiliation
                        left outer join azure_mag.author
                            on paperauthoraffiliation.authorid = author.authorid
                        left outer join azure_mag.affiliation
                            on paperauthoraffiliation.affiliationid = affiliation.affiliationid
                        left outer join azure_mag.gridinfo
                            on affiliation.gridid = gridinfo.gridid
                     group by paperauthoraffiliation.paperid) as pdetails
        on p.paperid = pdetails.paperid

where p.conferenceseriesid = 1163450153
