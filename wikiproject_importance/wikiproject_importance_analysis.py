import argparse
import bz2
import csv
import json

import pandas as pd

pd.set_option('display.max_rows', 100)

REMOVE = ['', 'NA', 'na', 'Unknown']
STANDARDIZE = {'top': 'High',
               'Top': 'High',
               'High': 'High',
               'high': 'High',
               'mid': 'Mid',
               'Mid': 'Mid',
               'Related': 'Low',
               'Bottom': 'Low',
               'low': 'Low',
               'Low': 'Low'}

def complex():
    """Examine article importance in context of article topics.

    Example JSON item:
        {
         "article_revid": 946053466,
         "wp_templates": ["Anthroponymy"],
         "title": "Andresen",
         "qid": "Q21501897",
         "importance": ["Unknown"],
         "sitelinks": {"ruwiki": 644537, "enwiki": 19573423, "nowiki": 404179},
         "talk_revid": 563903965,
         "talk_pid": 39941226,
         "topics": ["Culture.Linguistics"]
         }
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--importance_json", help="Bzipped JSON file with importance ratings")
    args = parser.parse_args()
    fn = args.importance_json

    no_assessments = {}
    articles_per_topic = {}
    single_assessment = {}
    single_level = {}
    adjacent_levels = {}
    full_range = {}
    multiple_assessments = {}
    with bz2.open(fn, 'rt') as fin:
        for i, line in enumerate(fin, start=1):
            article_json = json.loads(line)
            article_importance_assessments = [STANDARDIZE[a] for a in article_json['importance'] if a not in REMOVE]
            article_importance_levels = set(article_importance_assessments)
            topics = article_json['topics'] + ['All Articles']
            for t in topics:
                articles_per_topic[t] = articles_per_topic.get(t, 0) + 1
                if len(article_importance_assessments) == 0:
                    no_assessments[t] = no_assessments.get(t, 0) + 1
                elif len(article_importance_assessments) == 1:
                    single_assessment[t] = single_assessment.get(t, 0) + 1
                elif len(article_importance_levels) == 1:
                    single_level[t] = single_level.get(t, 0) + 1
                    multiple_assessments[t] = multiple_assessments.get(t, 0) + 1
                elif 'High' in article_importance_levels and 'Low' in article_importance_levels:
                    full_range[t] = full_range.get(t, 0) + 1
                    multiple_assessments[t] = multiple_assessments.get(t, 0) + 1
                else:
                    adjacent_levels[t] = adjacent_levels.get(t, 0) + 1
                    multiple_assessments[t] = multiple_assessments.get(t, 0) + 1


    df = pd.DataFrame([articles_per_topic, no_assessments, single_assessment,
                       multiple_assessments, single_level, adjacent_levels, full_range]).T
    df.columns = ['n', 'no assess.', 'single',
                  'mult assess.', 'agreed', 'adjacent', 'full']
    for c in ['no assess.', 'single', 'mult assess.', 'agreed', 'adjacent', 'full']:
        df[c] = df[c] / df['n']

    df['n'] = df['n'].apply(lambda x: int(x))
    print(df)


def simple():
    """Quick script for gathering importance level counts and some basic statistics on ambiguity."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--importance_tsv", help="Simple TSV file with importance ratings")
    args = parser.parse_args()
    fn = args.importance_tsv

    levels = {}

    no_assessments = 0
    single_level = 0
    adjacent_levels = 0
    full_range = 0
    with open(fn, 'r') as fin:
        tsvreader = csv.reader(fin, delimiter='\t')
        header = next(tsvreader)
        ai_idx = header.index("importance")
        for i, line in enumerate(tsvreader, start=1):
            ai = line[ai_idx]
            standardized_assessments = set()
            for ai_assessment in ai.split("|"):
                if ai_assessment in REMOVE:
                    continue
                elif ai_assessment in STANDARDIZE:
                    sai_assessment = STANDARDIZE[ai_assessment]
                    levels[sai_assessment] = levels.get(sai_assessment, 0) + 1
                    standardized_assessments.add(sai_assessment)
                else:
                    print("Unexpected level '{0}' from line {1}: {2}".format(ai_assessment, i, line))
            if len(standardized_assessments) == 0:
                no_assessments += 1
            elif len(standardized_assessments) == 1:
                single_level += 1
            elif 'High' in standardized_assessments and 'Low' in standardized_assessments:
                full_range += 1
            else:
                adjacent_levels += 1

    assert len(levels) == 3
    print("Count of each articles at each level:")
    for l in ['Low', 'Mid', 'High']:
        print("{0}: {1}".format(l, levels[l]))

    print("\nTypes of ranges:")
    print("No assessments: {0} ({1:.3f})".format(no_assessments, no_assessments / i))
    print("Single assessment: {0} ({1:.3f})".format(single_level, single_level / i))
    print("Multiple assessments, one level apart: {0} ({1:.3f})".format(adjacent_levels, adjacent_levels / i))
    print("Full range (Low and High) of assessments: {0} ({1:.3f})".format(full_range, full_range / i))


if __name__ == "__main__":
    complex()