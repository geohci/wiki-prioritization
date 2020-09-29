import argparse
import bz2
from collections import defaultdict
import csv
import json
import os
import re
import time
import yaml

import requests

def exec_mariadb_stat2(query, db, filename=None, verbose=True):
    """Query MariaDB."""
    if db in DB_METADATA:
        node = DB_METADATA[db]['node']
    else:
        raise NotImplementedError("Don't know mapping of db {0} to mysql node.".format(db))
    cmd = ('mysql --defaults-extra-file=/etc/mysql/conf.d/analytics-research-client.cnf '
           '-h s{0}-analytics-replica.eqiad.wmnet -P 331{0} -A --database {1} -e "{2}"'.format(node, db, query))
    if filename:
        cmd = cmd + " > " + filename
    if verbose:
        print(' '.join(cmd.split()))
    ret = os.system(cmd)
    return ret

def exec_hive_stat2(query, filename=None, priority=False, verbose=True, nice=False, large=False):
    """Query Hive."""
    if priority:
        query = "SET mapreduce.job.queuename=priority;" + query
    elif large:
        query = "SET mapreduce.job.queuename=nice; SET mapreduce.map.memory.mb=4096;" + query # SET mapreduce.map.memory.mb=4096
    elif nice:
        query = "SET mapreduce.job.queuename=nice;" + query
        # if issues: SET mapred.job.queue.name=nice;
    cmd = """hive -e \" """ + query + """ \""""
    if filename:
        cmd = cmd + " > " + filename
    if verbose:
        print(' '.join(cmd.split()))
    ret = os.system(cmd)
    return ret

def norm_wp_name_ar(wp):
    """Normalize Arabic Wikipedia WikiProject names. Based on trial and error."""
    ns_local = 'ويكيبيديا'
    return re.sub("\s\s+", " ", wp.lower().replace(ns_local + ":", "").replace('مشروع ويكي', '').strip())

def norm_wp_name_en(wp):
    """Normalize English Wikipedia WikiProject names. Based on trial and error."""
    ns_local = 'wikipedia'
    wp_prefix = 'wikiproject'
    return re.sub("\s\s+", " ", wp.lower().replace(ns_local + ":", "").replace(wp_prefix, "").strip())

def norm_wp_name_hu(wp):
    """Normalize Hungarian Wikipedia WikiProject names. Based on trial and error."""
    ns_local = 'wikipédia'
    to_strip = [ns_local + ":", 'témájú', 'kapcsolatos', 'műhelyek', 'műhely', '-es ', '-', 'országgal', 'ország']
    hardcoded_matches = {'Wikipédia:Harry Potter-műhely':'Harry Potterrel kapcsolatos',
                         'Wikipédia:USA-műhely':'USA-val kapcsolatos',
                         'Wikipédia:Anime- és mangaműhely':'anime-manga témájú',
                         'Wikipédia:Első világháború műhely':'első világháborús témájú'}
    for m in hardcoded_matches:
        if wp == m:
            wp = hardcoded_matches[m]
    wp = wp.lower()
    for s in to_strip:
        wp = wp.replace(s, ' ')
    return re.sub("\s\s+", " ", wp.strip())

def norm_wp_name_fr(wp):
    """Normalize French Wikipedia WikiProject names. Based on trial and error."""
    ns_local = 'projet'
    return re.sub("\s\s+", " ", wp.lower().replace(ns_local + ':', "").strip())

def norm_wp_name_tr(wp):
    """Normalize Turkish Wikipedia WikiProject names. Based on trial and error."""
    ns_local = 'vikiproje'
    wp_prefix = 'vikipedi'
    return re.sub("\s\s+", " ", wp.lower().replace(wp_prefix, "").replace(ns_local, '').replace(':', '').strip())

def generate_wp_to_labels(wp_taxonomy):
    """Bulid map of WikiProject label -> inferred topics."""
    wp_to_labels = defaultdict(set)
    for wikiproject_name, label in _invert_wp_taxonomy(wp_taxonomy):
        wp_to_labels[norm_wp_name_en(wikiproject_name)].add(label)
    return wp_to_labels

def _invert_wp_taxonomy(wp_taxonomy, path=None):
    """Helper for building WikiProject -> labels mapping"""
    catch_all = None
    catch_all_wikiprojects = []
    for key, value in wp_taxonomy.items():
        path_keys = (path or []) + [key]
        if key[-1] == "*":
            # this is a catch-all
            catch_all = path_keys
            catch_all_wikiprojects.extend(value)
            continue
        elif isinstance(value, list):
            catch_all_wikiprojects.extend(value)
            for wikiproject_name in value:
                yield wikiproject_name, ".".join(path_keys)
        else:
            yield from _invert_wp_taxonomy(value, path=path_keys)
    if catch_all is not None:
        for wikiproject_name in catch_all_wikiprojects:
            yield wikiproject_name, ".".join(catch_all)

def get_topics(wikiprojects, topics_taxonomy, topic_counts):
    """Map WikiProject labels to topics. Track statistics."""
    topics = set()
    for wp in wikiprojects:
        for wp_part in wp.split('/'):
            if wp_part not in topic_counts:
                topic_counts[wp_part] = 0
            wp_part_normed = norm_wp_name_en(wp_part)
            for t in topics_taxonomy.get(wp_part_normed, {}):
                topics.add(t)
                topic_counts[wp_part] += 1
    return sorted(topics)

def chunk(pageids, batch_size=50):
    """Batch pageIDS into sets of 50 for the Mediawiki API."""
    chunks = []
    for i in range(0, len(pageids), batch_size):
        chunks.append([str(p) for p in pageids[i:i+batch_size]])
    return chunks

def get_sitelinks_wikiprojects(output_json):
    """Mapping of WikiProjects across languages."""

    # SPARQL endpoint
    all_wikiprojects_query = "https://query.wikidata.org/sparql?query=%23WikiProjects%0ASELECT%20%3Fitem%20%3FitemLabel%20%0AWHERE%20%0A%7B%0A%20%20%3Fitem%20wdt%3AP31%20wd%3AQ21025364.%0A%20%20SERVICE%20wikibase%3Alabel%20%7B%20bd%3AserviceParam%20wikibase%3Alanguage%20%22%5BAUTO_LANGUAGE%5D%2Cen%22.%20%7D%0A%7D&format=json"
    session = requests.session()
    result = session.get(url=all_wikiprojects_query)
    data = result.json()
    qids = set()
    print("{0} WikiProjects.".format(len(data['results']['bindings'])))
    for wp in data['results']['bindings']:
        qid = wp['item']['value'].split('/')[-1]
        qids.add(qid)

    print("{0} WikiProject QIDs".format(len(qids)))
    base_url = 'https://wikidata.org/w/api.php'
    base_params = {"action": "wbgetentities",
                   "props": "sitelinks",
                   "format": "json",
                   "formatversion": 2}
    sitelinks = {}
    with requests.session() as session:
        for qid_set in chunk(list(qids), 50):
            params = base_params.copy()
            params['ids'] = '|'.join(qid_set)
            res = session.get(url=base_url, params=params).json()
            for q in res['entities']:
                qid = res['entities'][q]['id']
                q_slinks = {k:res['entities'][q]['sitelinks'][k]['title'] for k in res['entities'][q].get('sitelinks', {})}
                sitelinks[qid] = {}
                sitelinks[qid]['qid'] = qid
                sitelinks[qid]['sitelinks'] = q_slinks
            time.sleep(1)  # be kind to API
    with open(output_json, 'w') as fout:
        for qid in sitelinks:
            fout.write(json.dumps(sitelinks[qid]) + '\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--page_assessments_tsv",
                        default='./page_assessments.tsv',
                        help='TSV file with page assessments data and non-wikidata metadata.')
    parser.add_argument("--page_assessments_db",
                        default='enwiki',
                        help='Database for page assessments data.')
    parser.add_argument("--pid_to_qid_snapshot",
                        default="latest",
                        help='Dump date in format YYYYMMDD')
    parser.add_argument("--pid_to_qid_tsv",
                        default="./resources/pid_to_qid.tsv",
                        help="TSV file with full dump of page IDs and QIDs")
    parser.add_argument("--topics_yaml",
                        default="/home/halfak/projects/drafttopic/datasets/wikiproject_taxonomy.20191212.yaml",
                        help="YAML file with mapping between canonical WikiProject name and associated topics.")
    parser.add_argument("--wikiprojects_sitelinks_json",
                        help="JSON file with mapping between WikiProjects across languages.")
    parser.add_argument("--output_json",
                        help="Bzipped JSON file that will contain article metadata WikiProject templates, and inferred topics.")
    args = parser.parse_args()
    db = args.page_assessments_db
    norm_fn = DB_METADATA[db]['norm']

    # get mapping of pageID to list of all associated WikiProjects via page_assessments table in MariaDB
    # gathers importance ratings but doesn't track which evaluation came from which WikiProject
    # takes at most a few minutes for English Wikipedia -- very fast for other languages
    sep = '||'
    if not os.path.exists(args.page_assessments_tsv):
        print("Gathering page assessments data and writing to:", args.page_assessments_tsv)
        start_time = time.time()
        query = """
        SELECT pa.pa_page_id AS article_pid,
               GROUP_CONCAT(DISTINCT pap.pap_project_title SEPARATOR '{0}') AS wp_templates,
               MAX(p.page_latest) AS article_revid,
               MAX(p.page_title) AS title,
               MAX(ptalk.page_id) AS talk_pid,
               MAX(ptalk.page_latest) AS talk_revid,
               GROUP_CONCAT(DISTINCT pa.pa_importance SEPARATOR '{0}') AS importance
          FROM page_assessments pa
         INNER JOIN page_assessments_projects pap
               ON (pa.pa_project_id = pap.pap_project_id)
         INNER JOIN page p
               ON (pa.pa_page_id = p.page_id AND p.page_namespace = 0 and p.page_is_redirect = 0)
         INNER JOIN page ptalk
               ON (p.page_title = ptalk.page_title AND ptalk.page_namespace = 1)
         GROUP BY pa.pa_page_id
        """.format(sep)
        exec_mariadb_stat2(query=query, db=db, filename=args.page_assessments_tsv, verbose=True)
        print("Page assessments complete after {0:.1f} minutes!".format((time.time() - start_time) / 60))

    pids_to_metadata = {}
    with open(args.page_assessments_tsv, 'r') as fin:
        tsvreader = csv.reader(fin, delimiter='\t')
        assert next(tsvreader) == ['article_pid', 'wp_templates', 'article_revid', 'title', 'talk_pid', 'talk_revid', 'importance']
        for line in tsvreader:
            pid = int(line[0])
            wp_templates = line[1].split(sep)
            rid = int(line[2])
            title = line[3]
            tpid = int(line[4])
            trid = int(line[5])
            imp = line[6].split(sep)
            pids_to_metadata[pid] = {'wp_templates':wp_templates,
                                     'article_revid':rid,
                                     'title':title,
                                     'talk_pid':tpid,
                                     'talk_revid':trid,
                                     'importance':imp}
    print("{0} pages with WikiProject assessments in {1}.".format(len(pids_to_metadata), db))

    # get data for QIDs / sitelinks
    if not os.path.exists(args.pid_to_qid_tsv):
        print("Gathering PID / QID mapping and writing to:", args.pid_to_qid_tsv)
        start_time = time.time()
        query = """
        SELECT item_id,
               page_id,
               wiki_db
          FROM wmf.wikidata_item_page_link
         WHERE snapshot = '{0}'
               AND page_namespace = 0
               AND wiki_db LIKE '%wiki' AND wiki_db <> 'specieswiki' AND wiki_db <> 'commonswiki'
        """.format(args.pid_to_qid_snapshot)
        exec_hive_stat2(query, filename=args.pid_to_qid_tsv, priority=False, verbose=True, nice=True, large=False)
        print("PID / QID mapping complete after {0:.1f} minutes!".format((time.time() - start_time) / 60))

    qid_to_pids = {}
    pid_to_qid = {}
    with open(args.pid_to_qid_tsv, 'r') as fin:
        tsvreader = csv.reader(fin, delimiter='\t')
        assert next(tsvreader) == ['item_id', 'page_id', 'wiki_db']
        for line in tsvreader:
            wiki_db = line[2]
            if wiki_db == db:
                qid = line[0]
                pid = int(line[1])
                qid_to_pids[qid] = {}
                pid_to_qid[pid] = qid
    print("{0} pages in {1} with Wikidata IDs".format(len(qid_to_pids), db))

    with open(args.pid_to_qid_tsv, 'r') as fin:
        tsvreader = csv.reader(fin, delimiter='\t')
        assert next(tsvreader) == ['item_id', 'page_id', 'wiki_db']
        for line in tsvreader:
            qid = line[0]
            if qid in qid_to_pids:
                pid = int(line[1])
                wiki_db = line[2]
                qid_to_pids[qid][wiki_db] = pid

    found = 0
    for pid in pids_to_metadata:
        if pid in pid_to_qid:
            found += 1
            qid = pid_to_qid[pid]
            pids_to_metadata[pid]['sitelinks'] = qid_to_pids[qid]
            pids_to_metadata[pid]['qid'] = qid
    print("{0} sitelink sets found out of {1}".format(found, len(pids_to_metadata)))

    with open(args.topics_yaml, 'r') as fin:
        taxonomy = yaml.safe_load(fin)

    wikiproject_to_topic = generate_wp_to_labels(taxonomy)
    topics = set()
    for wp in wikiproject_to_topic:
        for topic in wikiproject_to_topic[wp]:
            topics.add(topic)

    if db != 'enwiki':
        get_sitelinks_wikiprojects(args.wikiprojects_sitelinks_json)
        db_to_enwiki = {}
        with open(args.wikiprojects_sitelinks_json, 'r') as fin:
            for line in fin:
                lj = json.loads(line)
                if 'enwiki' in lj['sitelinks'] and db in lj['sitelinks']:
                    db_to_enwiki[norm_fn(lj['sitelinks'][db])] = lj['sitelinks']['enwiki']
    print("{0} WikiProjects and {1} topics".format(len(wikiproject_to_topic), len(topics)))

    # dump articles to bzipped JSON with metadata and associated topics
    topic_counts = {}
    topic_dist = {}
    with bz2.open(args.output_json, 'wt') as fout:
        for pid in pids_to_metadata:
            wp_templates = pids_to_metadata[pid]['wp_templates']
            if db != 'enwiki':
                wp_templates = [db_to_enwiki[norm_fn(t)] for t in wp_templates if norm_fn(t) in db_to_enwiki]
            topics = get_topics(wp_templates, wikiproject_to_topic, topic_counts)
            topic_dist[len(topics)] = topic_dist.get(len(topics), 0) + 1
            pids_to_metadata[pid]['topics'] = topics
            fout.write(json.dumps(pids_to_metadata[pid]) + "\n")

    topic_counts = [(t, topic_counts[t]) for t in sorted(topic_counts, key=topic_counts.get, reverse=True)]
    if db == 'enwiki':
        topic_counts = [t[0] for t in topic_counts if
                        t[1] == 0 and 'task' not in t[0].lower() and 'force' not in t[0].lower()]
        print("WikiProjects w/o topics:", sorted(topic_counts))
    else:
        topic_counts = [t[0] for t in topic_counts if
                        t[1] > 0 and 'task' not in t[0].lower() and 'force' not in t[0].lower()]
        print("WikiProjects w/ topics:", sorted(topic_counts))

    print("Topic distribution:", topic_dist)

DB_METADATA = {'enwiki':{'node':1, 'norm':norm_wp_name_en},  # 21M pages
               'frwiki':{'node':6, 'norm':norm_wp_name_fr},  #  2.7M pages
               'arwiki':{'node':7, 'norm':norm_wp_name_ar},  #  2.8M pages
               'huwiki':{'node':7, 'norm':norm_wp_name_hu},  #    330K pages
               'trwiki':{'node':2, 'norm':norm_wp_name_tr}   #    280K pages
               }

if __name__ == "__main__":
    main()
