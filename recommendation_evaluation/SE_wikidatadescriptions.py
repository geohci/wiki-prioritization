import argparse
import time

import mwapi
import requests

GENDER_QID_TO_LABEL = {'Q6581097':'Man', 'Q6581072':'Woman'}

def filter_articles(candidates, reasons):
    """Filter articles to allowed set for recommendations.

    Criteria:
    * Not disambiguation page
    * Has associated Wikidata item
    * No existing description (either via Wikidata or local shortdesc)
    * Not protected in any way
    """
    filtered_candidates = {}
    for c in candidates:
        if 'pageprops' not in c:
            print("Missing pageprops:", c)
            reasons['missing'] += 1
            continue
        elif 'disambiguation' in c['pageprops']:
            reasons['disambiguation'] += 1
            continue
        elif not 'wikibase_item' in c['pageprops']:
            reasons['wikibase_missing'] += 1
            continue
        elif 'description' in c:
            reasons['has_description'] += 1
            continue
        elif c['protection']:
            reasons['protected'] += 1
            continue
        else:
            filtered_candidates[c['pageid']] = c
    return filtered_candidates

def add_wdpp(articles, wdpp):
    """Join in Wikidata page protection information to articles."""
    pid_to_qid = {pid:articles[pid]['pageprops']['wikibase_item'] for pid in articles}
    for pid, qid in pid_to_qid.items():
        found = False
        for item in wdpp:
            if item['title'] == qid:
                articles[pid]['item_protection'] = item['protection']
                found = True
                break
        if not found:
            print("No article found for (?!?!):", articles[pid])
    return articles

def filter_protected_items(items_with_protect_info):
    """Remove articles whose Wikidata items are protected from recommendations."""
    recs = {}
    for pid in items_with_protect_info:
        if not items_with_protect_info[pid]['item_protection']:
            recs[pid] = items_with_protect_info[pid]
    return recs

def add_gender_data(candidates, wd_session, gdata):
    """Add gender data (P21) for Wikidata items if humans (P31:Q5)"""
    qids = '|'.join([c['pageprops']['wikibase_item'] for c in candidates if c.get('pageprops', {}).get('wikibase_item')])
    GENDER_QUERY_BASE = {
        'action': 'wbgetentities',
        'props': 'claims',
        'format': 'json',
        'formatversion': 2,
        'ids': qids
    }
    gender_data = wd_session.get(**GENDER_QUERY_BASE)
    for i in range(len(candidates)):
        c = candidates[i]
        qid = c.get('pageprops', {}).get('wikibase_item')
        if qid and qid in gender_data['entities']:
            entity = gender_data['entities'][qid]['claims']
            is_human = False
            gender = None
            for iof in entity.get('P31', []):
                if iof.get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id') == 'Q5':
                    is_human = True
                    break
            if is_human:
                gdata['humans'] = gdata.get('humans', 0) + 1
                if entity.get('P21'):
                    gender = entity['P21'][0].get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id')
            if gender:
                c['gender'] = gender
                gdata[gender] = gdata.get(gender, 0) + 1
        else:
            print("Missing from gender data:", c)

def add_region_data(candidates, gdata):
    """Add gender data (P21) for Wikidata items if humans (P31:Q5)"""
    qids = '|'.join([c['pageprops']['wikibase_item'] for c in candidates if c.get('pageprops', {}).get('wikibase_item')])
    REGION_QUERY_BASE = {
        'qid': qids
    }
    session = requests.Session()
    region_data = session.get(url='https://wiki-region.wmcloud.org/api/v1/region', params=REGION_QUERY_BASE).json()
    region_data = {r['qid']:r['regions'] for r in region_data if r['regions']}
    for i in range(len(candidates)):
        c = candidates[i]
        qid = c.get('pageprops', {}).get('wikibase_item')
        if qid and qid in region_data:
            c['regions'] = region_data[qid]
            gdata['regions'] = gdata.get('regions', 0) + 1
            for region in region_data[qid]:
                gdata[region] = gdata.get(region, 0) + 1

def wikidata_description_add(iter=1, lang='en'):
    """Simulates process of generating Wikidata items to be recommended for descriptions in the Android App.
    Based on this code: https://github.com/wikimedia/mediawiki-services-recommendation-api/blob/master/lib/description.js

    Parameters:
        iter: number of recommendation sets to test. Multiply this number by 50 to get total number of candidates considered.
        lang: target wiki for descriptions -- e.g., en -> English Wikipedia; ar -> Arabic Wikipedia
    """
    lang_session = mwapi.Session('https://{0}.wikipedia.org'.format(lang), user_agent='isaac@wikimedia.org | rec test')
    wd_session = mwapi.Session('https://wikidata.org', user_agent='isaac@wikimedia.org | rec test')
    CANDIDATE_QUERY_BASE = {
        'action': 'query',
        'generator': 'random',
        'redirects': 1,
        'grnnamespace': 0,
        'grnlimit': 50,
        'prop': 'pageprops|description|info',
        'inprop': 'protection',
        'formatversion':2,
        'format':'json'
    }

    num_candidates = 0
    num_items = 0
    num_recs = 0
    reasons = {'missing':0, 'disambiguation':0, 'wikibase_missing':0, 'has_description':0, 'protected':0}
    candidate_gdata = {}
    candidate_rdata = {}
    rec_gdata = {}
    rec_rdata = {}
    for iter_idx in range(iter):
        print("== Iteration #{0}/{1} ==".format(iter_idx + 1, iter))
        # generate candidates and add in gender data
        candidates = lang_session.get(**CANDIDATE_QUERY_BASE)
        candidates = candidates['query']['pages']
        add_gender_data(candidates, wd_session, candidate_gdata)
        add_region_data(candidates, candidate_rdata)
        num_candidates += len(candidates)

        # filter articles to acceptable Wikidata items
        items = filter_articles(candidates, reasons)
        num_items += len(items)
        WDPP_QUERY_BASE = {
            'action': 'query',
            'prop': 'info',
            'inprop': 'protection',
            'formatversion': 2,
            'format': 'json',
            'titles': '|'.join([items[pid]['pageprops']['wikibase_item'] for pid in items])
        }

        # add in Wikidata protection info
        wdpp = wd_session.get(**WDPP_QUERY_BASE)
        wdpp = wdpp['query']['pages']
        items_with_protect_info = add_wdpp(items, wdpp)

        # filter to non-protected Wikidata items
        items_to_rec = filter_protected_items(items_with_protect_info)
        num_recs += len(items_to_rec)
        for qid in items_to_rec:
            if items_to_rec[qid].get('gender'):
                rec_gdata['humans'] = rec_gdata.get('humans', 0) + 1
                rec_gdata[items_to_rec[qid]['gender']] = rec_gdata.get(items_to_rec[qid]['gender'], 0) + 1
            if items_to_rec[qid].get('regions'):
                rec_rdata['regions'] = rec_rdata.get('regions', 0) + 1
                for r in items_to_rec[qid]['regions']:
                    rec_rdata[r] = rec_rdata.get(r, 0) + 1

        time.sleep(1)

    print("\nFinal statistics:")
    print("Started with {0} candidates".format(num_candidates))
    print("Filtered to {0} items ({1:.1f}% of candidates)".format(num_items, 100 * num_items / num_candidates))
    print("Details about why articles were filtered:")
    for r in reasons:
        print("\t{0}:\t{1} ({2:.1f}%)".format(r, reasons[r], 100 * reasons[r] / num_candidates))
    print("Filter to {0} recs ({1:.1f}% of images)".format(num_recs, 100 * num_recs / num_items))

    print("\nGender data:")
    print("{0} candidates and {1} were humans with gender info:".format(num_candidates, candidate_gdata['humans']))
    for g in sorted(candidate_gdata, key=candidate_gdata.get, reverse=True):
        if g != 'humans':
            print("\t{0}: {1} ({2:.1f}% of humans)".format(GENDER_QID_TO_LABEL.get(g, g), candidate_gdata[g],
                                                           100 * candidate_gdata[g] / candidate_gdata['humans']))
    print("{0} recommendations and {1} were humans with gender info:".format(num_recs, rec_gdata['humans']))
    for g in sorted(rec_gdata, key=rec_gdata.get, reverse=True):
        if g != 'humans':
            print("\t{0}: {1} ({2:.1f}% of humans)".format(GENDER_QID_TO_LABEL.get(g, g), rec_gdata[g],
                                                           100 * rec_gdata[g] / rec_gdata['humans']))

    print("\nRegion data:")
    print("{0} candidates and {1} were articles with relevant regions:".format(num_candidates, candidate_rdata['regions']))
    for r in sorted(candidate_rdata, key=candidate_rdata.get, reverse=True):
        if r != 'regions':
            print("\t{0}: {1} ({2:.1f}% of regions)".format(r, candidate_rdata[r],
                                                           100 * candidate_rdata[r] / candidate_rdata['regions']))
    print("{0} recommendations and {1} were articles with relevant regions:".format(num_recs, rec_rdata['regions']))
    for r in sorted(rec_rdata, key=rec_rdata.get, reverse=True):
        if r != 'regions':
            print("\t{0}: {1} ({2:.1f}% of regions)".format(r, rec_rdata[r],
                                                           100 * rec_rdata[r] / rec_rdata['regions']))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_calls", default=1, type=int)
    parser.add_argument("--lang", default='en')
    args = parser.parse_args()

    wikidata_description_add(args.num_calls, args.lang)


if __name__ == "__main__":
    main()