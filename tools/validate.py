#!/usr/bin/env python3
"""Check every problem article against ARTICLE_GUIDE.md: front matter, required headings in order,
at least three diagrams (one of them a sequence diagram) and a sane length. Usage: tools/validate.py [slug ...]"""
import json, os, re, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
problems = {p['slug']: p for p in json.load(open(os.path.join(ROOT, '_data', 'problems.json'), encoding='utf-8'))}
REQUIRED = ['The design at a glance', 'How to spend the 30 minutes', 'Requirements', 'Back-of-the-envelope numbers',
            'API', 'Data model', 'Design v1: the simplest thing that works', 'Where v1 breaks', 'Design v2:',
            'Key flow:', 'Deep dives', 'Trade-offs to say out loud', 'Follow-up questions', '60-second recap']
slugs = sys.argv[1:] or [s for s, p in problems.items() if p['id'] > 0 and os.path.exists(os.path.join(ROOT, 'p', s + '.md'))]
bad = 0
for slug in slugs:
    p, errs = problems[slug], []
    txt = open(os.path.join(ROOT, 'p', slug + '.md'), encoding='utf-8').read()
    fm = re.match(r'^---\n(.*?)\n---\n', txt, re.S)
    if not fm: errs.append('no front matter')
    else:
        for key in ('title', 'id', 'slug', 'aka', 'category', 'difficulty'):
            m = re.search(rf'^{key}:\s*"?(.*?)"?\s*$', fm.group(1), re.M)
            if not m or m.group(1) != str(p[key]): errs.append(f'front matter {key} should be {p[key]!r}')
    body = txt[fm.end():] if fm else txt
    if not body.lstrip().startswith('> **In one line:**'): errs.append('missing "> **In one line:**" opener')
    heads = re.findall(r'^## (.+)$', re.sub(r'```.*?```', '', body, flags=re.S), re.M)
    pos = -1
    for want in REQUIRED:
        idx = next((i for i, h in enumerate(heads) if h.startswith(want)), None)
        if idx is None: errs.append(f'missing heading "## {want}"')
        elif idx < pos: errs.append(f'heading "## {want}" out of order')
        else: pos = idx
    blocks = re.findall(r'```mermaid\n(.*?)```', body, re.S)
    if len(blocks) < 3: errs.append(f'only {len(blocks)} diagrams (need 3+)')
    if not any(b.lstrip().startswith('sequenceDiagram') for b in blocks): errs.append('no sequence diagram')
    first = re.search(r'## The design at a glance\s*\n+```mermaid', body)
    if not first: errs.append('"The design at a glance" must start with a diagram')
    words = len(re.sub(r'```.*?```', '', body, flags=re.S).split())
    if not 2000 <= words <= 4500: errs.append(f'{words} words (aim for 2,500-3,500)')
    print(f'{"OK  " if not errs else "FAIL"} {slug}: {words} words, {len(blocks)} diagrams' + ''.join(f'\n     - {e}' for e in errs))
    bad += bool(errs)
sys.exit(1 if bad else 0)
