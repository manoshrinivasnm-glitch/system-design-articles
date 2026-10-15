#!/usr/bin/env python3
"""Generate index.md and the problem table in README.md from _data/problems.json and the articles in p/.
Also stamps prev/next navigation into each article's front matter. Run before every commit."""
import html, json, os, re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
problems = json.load(open(os.path.join(ROOT, '_data', 'problems.json'), encoding='utf-8'))
SITE = 'https://manoshrinivasnm-glitch.github.io/system-design-articles'

def path(p):
    return os.path.join(ROOT, 'p', f"{p['slug']}.md")

written = [p for p in problems if os.path.exists(path(p))]
missing = [p for p in problems if p not in written]
guide = next((p for p in written if p['id'] == 0), None)
articles = [p for p in written if p['id'] > 0]
e = html.escape

SDE2_HEADING = 'Common SDE-2 Designs (ChatGPT suggested)'
course = [p for p in articles if p['group'] == 'course']
sde2 = sorted([p for p in articles if 'sde2' in p], key=lambda p: p['sde2'])

def link(p):
    return f'{{{{ "/p/{p["slug"]}.html" | relative_url }}}}'

def html_table(ps, mark_course=False):
    out = ['<table class="prob-table"><thead><tr><th>#</th><th>Problem</th><th>Level</th><th>Key ideas</th></tr></thead><tbody>']
    for p in ps:
        note = ' · in the course' if mark_course and p['group'] == 'course' else ''
        out.append(
            f'<tr><td>{p["id"]}</td>'
            f'<td><a href="{link(p)}">{e(p["title"])}</a><br><span class="ideas">{e(p["aka"])}{note}</span></td>'
            f'<td><span class="badge badge-{p["difficulty"].lower()}">{p["difficulty"]}</span></td>'
            f'<td class="ideas">{e(p["ideas"])}</td></tr>')
    out.append('</tbody></table>')
    return out

# index.md: guide card, the course problems, then the SDE-2 set
lines = ['---', 'title: All problems', 'permalink: /', '---',
         f'<p>{len(articles)} system design problems, each written as a 30-minute interview script: '
         'requirements, numbers, a simple v1, where it breaks, the scalable design, and the deep dives interviewers ask for. '
         'Every article opens with the final architecture diagram.</p>']
if guide:
    lines += ['<div class="guide-card">',
              f'<p><strong>New here? Start with <a href="{link(guide)}">{e(guide["title"])}</a>.</strong></p>',
              f'<p class="ideas">{e(guide["ideas"])}</p>', '</div>']
lines += [f'<h2 id="course">System Design Course ({len(course)} problems)</h2>'] + html_table(course)
lines += [f'<h2 id="sde2">{SDE2_HEADING}</h2>',
          '<p class="ideas">The designs asked most often in SDE-2 rounds. The ones the course already covers link to the same article.</p>']
lines += html_table(sde2, mark_course=True)
open(os.path.join(ROOT, 'index.md'), 'w', encoding='utf-8').write('\n'.join(lines) + '\n')

# README.md: replace each marked block with a Markdown table
def md_table(ps, mark_course=False):
    rows = ['| # | Problem | Level | Read |', '|---|---|---|---|']
    for p in ps:
        note = ' · in the course' if mark_course and p['group'] == 'course' else ''
        rows.append(f'| {p["id"]} | {p["title"]} ({p["aka"]}){note} | {p["difficulty"] or "Guide"} | '
                    f'[site]({SITE}/p/{p["slug"]}.html) · [markdown](p/{p["slug"]}.md) |')
    return '\n'.join(rows)

readme_path = os.path.join(ROOT, 'README.md')
if os.path.exists(readme_path):
    readme = open(readme_path, encoding='utf-8').read()
    for name, table in (('course', md_table(([guide] if guide else []) + course)), ('sde2', md_table(sde2, mark_course=True))):
        readme = re.sub(rf'(<!-- {name}:start -->\n).*?(<!-- {name}:end -->)',
                        lambda m: m.group(1) + table + '\n' + m.group(2), readme, flags=re.S)
    open(readme_path, 'w', encoding='utf-8').write(readme)

# prev/next stamping, in course order (the guide comes first)
for i, p in enumerate(written):
    txt = open(path(p), encoding='utf-8').read()
    m = re.match(r'^---\n(.*?)\n---\n', txt, re.S)
    if not m:
        print('WARN no front matter:', path(p)); continue
    fm = re.sub(r'^(prev_slug|prev_title|next_slug|next_title):.*\n?', '', m.group(1) + '\n', flags=re.M).rstrip('\n')
    extra = []
    if i > 0:
        q = written[i - 1]; extra += [f'prev_slug: {q["slug"]}', f'prev_title: "{q["title"]}"']
    if i < len(written) - 1:
        q = written[i + 1]; extra += [f'next_slug: {q["slug"]}', f'next_title: "{q["title"]}"']
    new = '---\n' + fm + ('\n' + '\n'.join(extra) if extra else '') + '\n---\n' + txt[m.end():]
    if new != txt:
        open(path(p), 'w', encoding='utf-8').write(new)

print(f'{len(articles)} articles + {1 if guide else 0} guide written'
      + (f'; missing: {", ".join(p["slug"] for p in missing)}' if missing else ''))
