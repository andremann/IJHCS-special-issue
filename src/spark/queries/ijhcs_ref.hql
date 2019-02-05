select p.paperid, ref.paperid as citingid, p.doi, p.papertitle, p.year, p.citationcount,
       j.normalizedname as journame,
       cs.normalizedname as confseries,
       ci.normalizedname as confname, ci.location as confplace,
       ptopics.topics,
       pdetails.authors 
    --    abstr.abstract,
    -- purl.urls
from azure_mag.paper as p
    join azure_mag.paperreference as ref
        on ref.paperreferenceid = p.paperid

    -- left outer join azure_mag.paperabstract as abstr
        -- on p.paperid = abstr.paperid

    left outer join azure_mag.journal as j
        on p.journalid = j.journalid

    left outer join azure_mag.conferenceinstance as ci
        on p.conferenceinstanceid = ci.conferenceinstanceid

    left outer join azure_mag.conferenceseries as cs
        on p.conferenceseriesid = cs.conferenceseriesid
    
    -- left outer join (select paperid, collect_set(sourceurl) as urls
    --                  from azure_mag.paperurl
    --                  group by paperid) as purl
    --     on p.paperid = purl.paperid
    
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

where ref.paperid in (select ijhcs.paperid
                      from amannocci.ijhcs as ijhcs)