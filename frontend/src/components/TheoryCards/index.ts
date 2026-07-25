interface CardDef {
  title: string
  summary: string
  formula: string
  detail: string
}

const CARDS: CardDef[] = [
  {
    title: 'Lamport Clock',
    summary: 'Lamport\'s scalar clock establishes a partial order using increment-and-carry timestamps.',
    formula: 'C(a) < C(b)  if  a → b',
    detail: 'Each process increments its counter before each event; messages carry the sender\'s timestamp; the receiver sets its clock to max(local, received) + 1. If a → b then C(a) < C(b), but the converse does not hold — concurrent events may have arbitrary clock values.',
  },
  {
    title: 'Vector Clock',
    summary: 'One counter per process: each event carries the full vector, capturing the exact partial order.',
    formula: 'VC(a) < VC(b)  iff  ∀k: a[k] ≤ b[k] ∧ ∃k\': a[k\'] < b[k\']',
    detail: 'A vector clock assigns a vector of N counters (one per process) to each event. Charron-Bost showed that vector clocks capture the exact partial order, unlike Lamport clocks. Two events are concurrent iff neither vector dominates the other.',
  },
  {
    title: 'Matrix Clock',
    summary: 'An N×N matrix where M[i][j] is process i\'s knowledge of process j\'s clock.',
    formula: 'M_i[k][l] = knowledge of P_k\'s VC by P_i',
    detail: 'Matrix clocks extend vector clocks: each process maintains an N×N matrix M where M[i][j] is i\'s knowledge of j\'s clock. After a message from Pj, process Pi updates M[i][j] = max(M[i][j], M[j][j]). Used for distributed garbage collection and knowledge-based protocols.',
  },
  {
    title: 'BSS Causal Delivery',
    summary: 'Birman-Schiper-Stephenson: delay delivery until all causal predecessors are received.',
    formula: 'deliver(m)  when  m[j] = VC_j + 1  ∧  ∀k≠j: m[k] ≤ VC_k',
    detail: 'Messages carry the sender\'s vector clock. A message from Pj is delivered only when: (1) msg[j] = VC[j] + 1 (it\'s the next expected from Pj), and (2) ∀k ≠ j: msg[k] ≤ VC[k] (all causal predecessors have been delivered). Otherwise the message is buffered.',
  },
  {
    title: 'Chandy-Lamport Snapshot',
    summary: 'FIFO-channel global snapshot using marker messages.',
    formula: 'state = ∪ (local_state_i, channel_state_{ij})',
    detail: 'Chandy & Lamport (1985): a global snapshot algorithm for FIFO channels. An initiator records its state and sends markers on all outgoing channels. A process receiving its first marker records its state and forwards markers; subsequent markers finalize that incoming channel\'s state.',
  },
  {
    title: 'Conflict Detection',
    summary: 'Two writes are concurrent when neither vector clock dominates the other.',
    formula: 'VC(x) ∥ VC(y)  iff  ¬(x < y) ∧ ¬(y < x)',
    detail: 'Causal conflicts occur when two writes are concurrent (neither VC dominates the other). The conflict detector creates sibling versions; conflicts are resolved via last-writer-wins, first-writer-wins, or custom merge functions. Concurrent writes are detected in the DHT layer.',
  },
  {
    title: 'Singhal-Kshemkalyani Diff',
    summary: 'Send only the components of the vector clock that changed since the last message.',
    formula: 'diff_i = {k → VC_i[k] | VC_i[k] ≠ last_sent_{i→j}[k]}',
    detail: 'The DiffVC optimization (Singhal & Kshemkalyani 1992): instead of sending the full vector clock with every message, send only the components that changed since the last message to that process. The receiver applies the diff to reconstruct the full VC. This reduces bandwidth in high-fanout systems.',
  },
]

function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

export class TheoryCards {
  private container: HTMLElement

  constructor(container: HTMLElement) {
    this.container = container
    this.render()
  }

  private render(): void {
    let html = '<div class="space-y-2">'
    for (let i = 0; i < CARDS.length; i++) {
      const card = CARDS[i]
      const id = `theory-card-${i}`
      html += `
<div class="bg-slate-800 border border-slate-700 rounded text-xs transition-colors hover:border-slate-600">
  <button
    type="button"
    class="w-full flex items-center justify-between px-3 py-2 text-left"
    data-card-toggle="${id}"
  >
    <span class="font-semibold text-slate-200">${esc(card.title)}</span>
    <span class="text-slate-400 text-sm leading-none" data-card-indicator="${id}">+</span>
  </button>
  <div id="${id}" class="hidden px-3 pb-3 space-y-1.5">
    <p class="text-slate-400 leading-relaxed">${esc(card.summary)}</p>
    <p class="font-mono text-amber-300 text-[11px]">${esc(card.formula)}</p>
    <p class="text-slate-500 leading-relaxed">${esc(card.detail)}</p>
  </div>
</div>`
    }
    html += '</div>'
    this.container.innerHTML = html

    this.container.querySelectorAll('[data-card-toggle]').forEach(btn => {
      btn.addEventListener('click', () => {
        const id = (btn as HTMLElement).getAttribute('data-card-toggle')!
        const body = document.getElementById(id)
        const indicator = this.container.querySelector(`[data-card-indicator="${id}"]`)
        if (!body) return
        const isOpen = !body.classList.contains('hidden')
        body.classList.toggle('hidden')
        if (indicator) {
          indicator.textContent = isOpen ? '+' : '−'
        }
      })
    })
  }
}
