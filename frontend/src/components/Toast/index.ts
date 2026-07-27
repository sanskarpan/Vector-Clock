let container: HTMLDivElement | null = null

function getContainer(): HTMLDivElement {
  if (!container) {
    container = document.createElement('div')
    container.id = 'toast-container'
    container.style.cssText = [
      'position:fixed', 'bottom:1.5rem', 'right:1.5rem',
      'z-index:9999', 'display:flex', 'flex-direction:column',
      'gap:0.5rem', 'max-width:360px'
    ].join(';')
    document.body.appendChild(container)
  }
  return container
}

export function showToast(message: string, type: 'error' | 'info' | 'success' = 'error'): void {
  const c = getContainer()
  const el = document.createElement('div')

  const colors = {
    error:   'background:#450a0a;border-color:#991b1b;color:#fca5a5',
    info:    'background:#0c1a2e;border-color:#1d4ed8;color:#93c5fd',
    success: 'background:#052e16;border-color:#166534;color:#86efac',
  }

  el.style.cssText = [
    'display:flex', 'align-items:flex-start', 'gap:0.5rem',
    'padding:0.6rem 0.9rem', 'border-radius:0.375rem', 'border:1px solid',
    'font-size:0.75rem', 'line-height:1.4', 'cursor:pointer',
    'animation:toast-in 0.2s ease',
    colors[type]
  ].join(';')

  const style = document.createElement('style')
  style.textContent = '@keyframes toast-in{from{opacity:0;transform:translateX(1rem)}to{opacity:1;transform:none}}'
  if (!document.head.querySelector('#toast-style')) {
    style.id = 'toast-style'
    document.head.appendChild(style)
  }

  const text = document.createElement('span')
  text.style.flex = '1'
  text.textContent = message
  el.appendChild(text)

  const close = document.createElement('button')
  close.textContent = '×'
  close.style.cssText = 'background:none;border:none;cursor:pointer;font-size:1rem;line-height:1;opacity:0.7;padding:0;color:inherit'
  close.addEventListener('click', () => el.remove())
  el.appendChild(close)

  el.addEventListener('click', (e) => {
    if (e.target !== close) el.remove()
  })

  c.appendChild(el)

  setTimeout(() => {
    el.style.transition = 'opacity 0.3s'
    el.style.opacity = '0'
    setTimeout(() => el.remove(), 310)
  }, 5000)
}
