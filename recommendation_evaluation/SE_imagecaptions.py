import argparse
import time

import mwapi

GENDER_QID_TO_LABEL = {'Q6581097':'Man', 'Q6581072':'Woman'}

def filter_images(candidates):
    """Return only images -- i.e. remove audio files etc."""
    filtered_candidates = {}
    for c in candidates:
        if 'imageinfo' not in c or not c['imageinfo']:
            print("Missing imageinfo:", c)
            continue
        if c['imageinfo'][0]['mime'].startswith('image'):
            filtered_candidates[c['pageid']] = c
    return filtered_candidates

def filter_protections(candidates):
    """Return only non-protected pages"""
    non_protected_candidates = {}
    for c in candidates:
        if 'protection' not in candidates[c]:
            print("Missing protection info:", candidates[c])
            continue
        if not candidates[c]['protection']:
            non_protected_candidates[c] = candidates[c]
    return non_protected_candidates

def add_sd(images, sd, counts):
    """Add in existing caption info from Commons"""
    pids = [pid for pid in images.keys()]
    for pid in pids:
        mid = 'M{0}'.format(pid)
        if mid in sd:
            # no structured data exists for the image
            if 'missing' in sd[mid]:
                images[pid]['sd'] = 'missing'
                counts['missing'] += 1
            # caption already exists in language
            elif sd[mid]['labels']:
                images[pid]['sd'] = 'exists'
                counts['exists'] += 1
            # some structured data for the image exists but not the caption in the right language
            else:
                images[pid]['sd'] = 'none'
                counts['none'] += 1
        # API failed to include result for some reason
        else:
            images[pid]['sd'] = 'N/A'
            counts['N/A'] += 1
    return images

def filter_captions(images_with_sd):
    """Remove images that already have captions from recommendations."""
    recs = {}
    for i in images_with_sd:
        if images_with_sd[i]['sd'] != 'exists':
            recs[i] = images_with_sd[i]
    return recs

def chunkify(input_list, max_size):
    """Break list of items in chunks of a given size for API input."""
    for i in range(0, len(input_list), max_size):
        yield input_list[i:i+max_size]

def gender_stats_images(candidate_articles, articles_recommended, lang):
    """Gather gender data about candidate and recommended images.

    Steps:
    * Takes sets of articles that are associated with the image candidates/recommendations
    * For each article, checks associated Wikidata item to see if human (P31:Q5) and records gender (P21)
    * Computes aggregate gender stats for candidates and recommended images based on this info
    """
    wd_session = mwapi.Session('https://wikidata.org', user_agent='isaac@wikimedia.org | rec test')
    c_gender = {}
    r_gender = {}

    for ca in chunkify(list(candidate_articles), 50):
        GENDER_QUERY_BASE = {
            'action': 'wbgetentities',
            'props': 'claims|sitelinks',
            'format': 'json',
            'formatversion': 2,
            'sites':'{0}wiki'.format(lang),
            'titles': '|'.join(ca)
        }

        gender_data = wd_session.get(**GENDER_QUERY_BASE)
        for qid in gender_data['entities']:
            entity = gender_data['entities'][qid]
            title = entity['sitelinks']['enwiki']['title']
            claims = entity['claims']
            is_human = False
            gender = None
            for iof in claims.get('P31', []):
                if iof.get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id') == 'Q5':
                    is_human = True
                    break
            if is_human:
                if claims.get('P21'):
                    gender = claims['P21'][0].get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id')
            if gender:
                c_gender[gender] = c_gender.get(gender, 0) + 1
                if title.replace(" ", "_") in articles_recommended:
                    r_gender[gender] = r_gender.get(gender, 0) + 1

    print("\nGender data:")
    print("{0} candidates and {1} were humans with gender info:".format(len(candidate_articles), sum(c_gender.values())))
    for g in c_gender:
        print("\t{0}: {1} ({2:.1f}%)".format(GENDER_QID_TO_LABEL.get(g, g),
                                             c_gender[g], c_gender[g] / sum(c_gender.values())))
    print("{0} recommended and {1} were humans with gender info:".format(len(articles_recommended), sum(r_gender.values())))
    for g in r_gender:
        print("\t{0}: {1} ({2:.1f}%)".format(GENDER_QID_TO_LABEL.get(g, g),
                                             r_gender[g], r_gender[g] / sum(r_gender.values())))


def image_captions_add(iter=1, lang='en'):
    """Simulates process of generating images to be recommended for captions in the Android App.
    Based on this code: https://github.com/wikimedia/mediawiki-services-recommendation-api/blob/master/lib/caption.js

    Parameters:
        iter: number of recommendation sets to test. Multiply this number by 50 to get total number of candidates considered.
        lang: target wiki for captions -- e.g., en -> English Wikipedia; ar -> Arabic Wikipedia
    """
    session = mwapi.Session('https://commons.wikimedia.org', user_agent='isaac@wikimedia.org | rec test')
    CANDIDATE_QUERY_BASE = {
        'action': 'query',
        'formatversion': 2,
        'generator': 'random',
        'redirects': '',
        'grnnamespace': 6,
        'grnlimit': 50,
        'prop': 'imageinfo|globalusage|info',
        'inprop': 'protection',
        'iiprop': 'timestamp|user|url|mime',
        'iiurlwidth': 320,
        'iilocalonly': '',
        'gunamespace': 0,
        'guprop': 'pageid',
        'format': 'json',
        'gusite': '{0}wiki'.format(lang)
    }

    num_candidates = 0
    num_inuse = 0
    num_inuse_recs = 0
    num_images = 0
    num_recs = 0
    sd_counts = {'missing':0, 'exists':0, 'none':0, 'N/A':0}
    candidate_articles = set()
    recommended_articles = set()
    for iter_idx in range(iter):
        print("== Iteration #{0}/{1} ==".format(iter_idx + 1, iter))
        # generate candidates
        candidates = session.get(**CANDIDATE_QUERY_BASE)
        candidates = candidates['query']['pages']
        num_candidates += len(candidates)

        # filter to images
        images = filter_images(candidates)
        for i in images:
            if images[i]['globalusage']:
                for s in images[i]['globalusage']:
                    candidate_articles.add(s['title'])
        editable_images = filter_protections(images)
        if len(images) != len(editable_images):
            print("\t =={0} removed for page protections==".format(len(images) - len(editable_images)))
        num_inuse += len([i for i in editable_images if editable_images[i]['globalusage']])
        num_images += len(editable_images)

        # add existing caption info
        SD_QUERY_BASE = {
            'action': 'wbgetentities',
            'props': 'labels',
            'format': 'json',
            'formatversion': 2,
            'ids': '|'.join(['M{0}'.format(pid) for pid in editable_images])
        }
        sd = session.get(**SD_QUERY_BASE)
        sd = sd['entities']
        images_with_sd = add_sd(editable_images, sd, sd_counts)

        # generate final recommendation set
        images_to_rec = filter_captions(images_with_sd)
        num_recs += len(images_to_rec)
        for i in images_to_rec:
            if images_to_rec[i]['globalusage']:
                num_inuse_recs += 1
                for s in images_to_rec[i]['globalusage']:
                    recommended_articles.add(s['title'])
        time.sleep(1)

    print("\nFinal statistics:")
    print("Started with {0} candidates".format(num_candidates))
    print("Filtered to {0} images ({1:.1f}% of candidates) -- {2} ({3:.1f}% of images) in use on {4}wiki".format(
        num_images, 100 * num_images / num_candidates, num_inuse, 100 * num_inuse / num_images, lang))
    print("Details about existing structured data on Commons for these images:")
    for c in sd_counts:
        print("\t{0}:\t{1} ({2:.1f}%)".format(c, sd_counts[c], 100 * sd_counts[c] / num_images))
    print("Filter to {0} recs ({1:.1f}% of images) -- {2} ({3:.1f}% of recs) in use on {4}wiki".format(
        num_recs, 100 * num_recs / num_images, num_inuse_recs, 100 * num_inuse_recs / num_recs, lang))

    gender_stats_images(candidate_articles, recommended_articles, lang)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_calls", default=1, type=int)
    parser.add_argument("--lang", default='en')
    args = parser.parse_args()

    image_captions_add(args.num_calls, args.lang)

if __name__ == "__main__":
    main()