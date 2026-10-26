// Поле для API-ключа в шапке страницы.
//
// Временное решение до переписывания фронтенда: ключ вводится вручную
// и хранится только в браузере (localStorage). В исходниках ключа нет.
// Страница получает ключ через getApiKey().
(function () {
  const STORAGE_KEY = 'callsApiKey';
  let memoryKey = '';

  function readKey() {
    try { return localStorage.getItem(STORAGE_KEY) || memoryKey; } catch (e) { return memoryKey; }
  }

  function writeKey(value) {
    memoryKey = value;
    try {
      if (value) localStorage.setItem(STORAGE_KEY, value);
      else localStorage.removeItem(STORAGE_KEY);
    } catch (e) { /* приватный режим: ключ живёт до перезагрузки */ }
  }

  window.getApiKey = readKey;

  const style = document.createElement('style');
  style.textContent = `
    .api-key-box { display: flex; align-items: center; gap: 8px; }
    .api-key-box label { font-size: 12px; color: var(--text-secondary); white-space: nowrap; }
    .api-key-box input {
      width: 220px; height: 38px; padding: 0 12px;
      border: 1px solid var(--border-color); border-radius: var(--radius-sm);
      background: var(--bg-card); color: var(--text-primary);
      font-family: monospace; font-size: 12px;
    }
    .api-key-box input:focus { outline: none; border-color: var(--accent-blue); }
    .api-key-box.missing input { border-color: var(--accent-red); }
    .api-key-box button {
      height: 38px; padding: 0 14px; cursor: pointer;
      border: 1px solid var(--border-color); border-radius: var(--radius-sm);
      background: var(--bg-card); color: var(--text-primary); font-size: 13px;
    }
    .api-key-box button:hover { border-color: var(--accent-blue); }
    @media (max-width: 900px) { .api-key-box label { display: none; } .api-key-box input { width: 140px; } }
  `;
  document.head.appendChild(style);

  function mount() {
    const host = document.querySelector('.topbar-right');
    if (!host) return;

    const box = document.createElement('form');
    box.className = 'api-key-box';
    box.innerHTML = `
      <label for="apiKeyInput">API-ключ</label>
      <input id="apiKeyInput" type="password" autocomplete="off" spellcheck="false"
             placeholder="Вставьте X-API-Key">
      <button type="submit">Применить</button>
    `;
    const input = box.querySelector('input');
    input.value = readKey();
    box.classList.toggle('missing', !input.value);

    box.addEventListener('submit', (e) => {
      e.preventDefault();
      writeKey(input.value.trim());
      // Данные страницы загружаются при старте — проще всего перезагрузить её с новым ключом.
      location.reload();
    });

    host.prepend(box);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount);
  else mount();
})();
