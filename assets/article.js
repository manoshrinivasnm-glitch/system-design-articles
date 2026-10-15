// Turns ```mermaid code blocks (rendered by kramdown as .language-mermaid) into diagrams.
// github.com renders the same blocks natively, so the Markdown stays readable in both places.
import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';

const dark = window.matchMedia('(prefers-color-scheme: dark)').matches;
const blocks = [...document.querySelectorAll('div.language-mermaid, pre > code.language-mermaid')];

for (const block of blocks) {
  const host = block.tagName === 'CODE' ? block.parentElement : block;
  const fig = document.createElement('figure');
  fig.className = 'diagram';
  const pre = document.createElement('pre');
  pre.className = 'mermaid';
  pre.textContent = host.textContent;
  fig.appendChild(pre);
  host.replaceWith(fig);
}

if (blocks.length) {
  mermaid.initialize({
    startOnLoad: false,
    theme: dark ? 'dark' : 'default',
    themeVariables: { fontSize: '15px' },
    flowchart: { htmlLabels: true, curve: 'basis', useMaxWidth: false, nodeSpacing: 35, rankSpacing: 45 },
    sequence: { mirrorActors: false, useMaxWidth: false },
  });
  await mermaid.run({ querySelector: 'pre.mermaid' });
  // Shrink a diagram to fit its box, but never below 70% so labels stay readable; past that the box scrolls sideways.
  const fit = () => {
    for (const svg of document.querySelectorAll('.diagram svg')) {
      const vb = svg.viewBox.baseVal;
      if (!vb || !vb.width) continue;
      const room = svg.closest('.diagram').clientWidth - 24;
      const scale = Math.max(0.7, Math.min(1, room / vb.width));
      svg.setAttribute('width', Math.round(vb.width * scale));
      svg.setAttribute('height', Math.round(vb.height * scale));
      svg.style.maxWidth = 'none';
    }
  };
  fit();
  window.addEventListener('resize', fit);
}
