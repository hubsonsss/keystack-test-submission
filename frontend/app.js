const API = '/api'

async function load() {
  const res = await fetch(`${API}/customers`)
  const customers = await res.json()
  const box = document.getElementById('customers')
  box.innerHTML = ''
  for (const c of customers) {
    const keys = await (await fetch(`${API}/customers/${c.id}/keys`)).json()
    const div = document.createElement('div')
    div.className = 'customer'
    div.innerHTML = `
      <strong>${c.name}</strong> ${c.email || ''}
      <button onclick="createKey(${c.id})">New key</button>
      <ul>
        ${keys.map((k) => `
          <li class="${k.status}">
            ${k.prefix}… — ${k.status}
            ${k.status === 'active' ? `<button onclick="revokeKey(${k.id})">Revoke</button>` : ''}
          </li>`).join('')}
      </ul>`
    box.appendChild(div)
  }
}

document.getElementById('customer-form').addEventListener('submit', async (e) => {
  e.preventDefault()
  await fetch(`${API}/create_customer`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      name: document.getElementById('name').value,
      email: document.getElementById('email').value || null,
    }),
  })
  e.target.reset()
  load()
})

async function createKey(customerId) {
  const res = await fetch(`${API}/customers/${customerId}/keys`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
  })
  const key = await res.json()
  const saved = JSON.parse(localStorage.getItem('keys') || '[]')
  saved.push(key)
  localStorage.setItem('keys', JSON.stringify(saved))
  const box = document.getElementById('new-key')
  box.textContent = `New key: ${key.secret}`
  box.hidden = false
  load()
}

async function revokeKey(id) {
  await fetch(`${API}/keys/${id}/revoke`, { method: 'POST' })
  load()
}

load()
