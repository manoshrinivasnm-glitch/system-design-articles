// Usage: node tools/check_diagrams.mjs file.md [file.md ...]  — parses every ```mermaid block, exits 1 on any error.
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
const dom = new JSDOM('<!doctype html><html><body></body></html>', { pretendToBeVisual: true });
globalThis.window = dom.window; globalThis.document = dom.window.document;
globalThis.DOMParser = dom.window.DOMParser; globalThis.Element = dom.window.Element;
globalThis.HTMLElement = dom.window.HTMLElement; globalThis.SVGElement = dom.window.SVGElement;
const { default: mermaid } = await import('mermaid');
mermaid.initialize({ startOnLoad: false });
let bad = 0, total = 0;
for (const f of process.argv.slice(2)) {
  const txt = fs.readFileSync(f, 'utf8');
  const re = /```mermaid\n([\s\S]*?)```/g; let m, i = 0;
  while ((m = re.exec(txt))) {
    i++; total++;
    const line = txt.slice(0, m.index).split('\n').length;
    try { await mermaid.parse(m[1]); }
    catch (e) { bad++; console.log(`FAIL ${f}:${line} (diagram ${i}): ${String(e.message || e).split('\n').slice(0, 4).join(' | ')}`); }
  }
  if (/\{\{|\{%/.test(txt.replace(/\{\{ *"[^"]*" *\| *relative_url *\}\}/g, ''))) { bad++; console.log(`FAIL ${f}: contains {{ or {% (Liquid will break the page)`); }
}
console.log(`${total - (bad > total ? total : bad)} / ${total} diagrams OK${bad ? `, ${bad} problem(s)` : ''}`);
process.exit(bad ? 1 : 0);
