raw_revlog_filename = './raw.revlog'

grouped_lines = []

with open(raw_revlog_filename, encoding='iso-8859-1') as f:
    group = []
    for line in f:
        if line == '\n':
            grouped_lines.append(group)
            group = []
        else:
            group.append(line.strip())


import re

def file_stats_from(description):
    match = re.search('(.*) +\| +(\d+) +(.*)', description, re.IGNORECASE)
    filename = match.group(1)
    n_lines_changed = int(match.group(2))
    changes = match.group(3)
    n_add = int(1.0 * n_lines_changed * changes.count('+') / len(changes))
    n_del = int(1.0 * n_lines_changed * changes.count('-') / len(changes))
    return [filename.strip(), str(n_add), str(n_del)]


import re

timestamps = []
commit_dates = []
hashes = []
names = []
emails = []
subjs = []
filenames = []
n_adds = []
n_dels = []

i = 0
for group in grouped_lines:
    if i % 50000 == 0:
        print("{}%% done".format(100.0*i/len(grouped_lines)))
    i+=1
    tsv, remaining = group[0].split("\t"), group[1:]
    # empty subjects
    if group[0].count('\t') == 4:
        ts_author, commit_date_iso, commit_hash, author_name, author_email, subj = tsv[0], tsv[1], tsv[2], tsv[3], tsv[4], "empty"
    else:
        ts_author, commit_date_iso, commit_hash, author_name, author_email, subj = tsv[0], tsv[1], tsv[2], tsv[3], tsv[4], tsv[5]

    # replace separator values from subject
    #subj = unicode(subj.replace(",", " "), 'utf-8')
    # subj = subj.replace(",", " ").decode('utf-8','ignore').encode("utf-8")
    subj = subj.replace(",", " ").encode("utf-8")

    # no renames, no binary changes
    file_changes = [r for r in remaining if '|' in r and '=>' not in r and '->' not in r and ('+' in ''.join(r.split('|')[1:]) or '-' in ''.join(r.split('|')[1:]))]
    details = [file_stats_from(changes) for changes in file_changes]

    for detail in details:
        timestamps.append(int(ts_author))
        commit_dates.append(commit_date_iso)
        hashes.append(commit_hash)
        names.append(author_name)
        emails.append(author_email)
        subjs.append(subj)
        filenames.append(detail[0])
        n_adds.append(int(detail[1]))
        n_dels.append(int(detail[2]))


import pandas as pd
import dateutil.parser
def tz_from(date_str):
    d = dateutil.parser.parse(date_str)
    hours_from_utc = d.tzinfo.utcoffset(d).total_seconds() / 3600.0
    return int(hours_from_utc)

df = pd.DataFrame(
    {
        'author_timestamp': timestamps,
        'commit_utc_offset_hours': map(tz_from, commit_dates),
        'commit_hash': hashes,
        'author_name': names,
        'author_email': emails,
        'subject': subjs,
        'filename': filenames,
        'n_additions': n_adds,
        'n_deletions': n_dels
    }
)


unique_author_ids = df.apply(lambda row: row['author_name'] + ' ' + row['author_email'], axis=1).unique()
translation = {author_id: index for index, author_id in enumerate(unique_author_ids)}
df['author_id'] = df.apply(lambda row: translation[row['author_name'] + ' ' + row['author_email']], axis=1)
df.drop('author_email', axis=1, inplace=True)
df.drop('author_name', axis=1, inplace=True)

df.to_csv('./kaggle_linux_kernel_git_revlog.csv', index=False)
