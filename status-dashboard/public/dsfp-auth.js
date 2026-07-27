/* ===========================================================================
 * DSFP dashboard-wide OAuth login.
 * ---------------------------------------------------------------------------
 * Provides a single Bearer-token session shared across every dashboard page:
 *   - injects a "Signed in / Sign in" widget into the nav,
 *   - opens a modal where the user enters DSFP credentials,
 *   - exposes window.DsfpAuth with helpers any page can call.
 *
 * The server keeps the actual token (and the credentials used for transparent
 * refresh) in memory; the browser only knows whether a session exists and who
 * the logged-in user is.
 * ======================================================================== */
(function () {
    'use strict';

    const STATUS_URL = '/api/dsfp/status';
    const LOGIN_URL = '/api/dsfp/login';
    const LOGOUT_URL = '/api/dsfp/logout';

    const state = {
        status: null,            // last server response from /status
        listeners: [],           // fn(status) callbacks
        modalEl: null,
        booted: false
    };

    // ---------- public API -------------------------------------------------
    const DsfpAuth = {
        /** Returns the cached status object (null until first refresh). */
        getStatus: () => state.status,

        /** Refresh status from the server and notify listeners. */
        refresh,

        /** Subscribe to status changes. Fires immediately if status is known. */
        on(fn) {
            if (typeof fn !== 'function') return () => {};
            state.listeners.push(fn);
            if (state.status) { try { fn(state.status); } catch (e) { /* ignore */ } }
            return () => { state.listeners = state.listeners.filter(x => x !== fn); };
        },

        /**
         * Ensure a session exists. If logged-out, opens the login modal and
         * resolves once the user has signed in (or rejects if cancelled).
         */
        async ensureLogin() {
            if (!state.status) await refresh();
            if (state.status && state.status.loggedIn) return state.status;
            return openLoginModal();
        },

        /** Programmatic login (returns the new status). */
        async login(credentials) {
            const resp = await fetch(LOGIN_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(credentials || {})
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.success) {
                throw new Error(data.error || `Login failed (HTTP ${resp.status})`);
            }
            await refresh();
            return state.status;
        },

        async logout() {
            try { await fetch(LOGOUT_URL, { method: 'POST' }); } catch (e) { /* ignore */ }
            await refresh();
        },

        openLogin: () => openLoginModal()
    };
    window.DsfpAuth = DsfpAuth;

    // ---------- status / rendering ----------------------------------------
    async function refresh() {
        try {
            const r = await fetch(STATUS_URL, { headers: { 'Accept': 'application/json' } });
            state.status = await r.json();
        } catch (e) {
            state.status = { loggedIn: false, config: {} };
        }
        renderWidget();
        notify();
        return state.status;
    }

    function notify() {
        state.listeners.slice().forEach(fn => { try { fn(state.status); } catch (e) { /* ignore */ } });
    }

    function renderWidget() {
        const slot = document.getElementById('dsfp-auth-widget');
        if (!slot) return;
        const s = state.status || { loggedIn: false };
        if (!s.loggedIn) {
            slot.innerHTML = `
                <button type="button" class="dsfp-auth-btn" onclick="DsfpAuth.openLogin()">
                    <i data-lucide="log-in"></i><span>Sign in to DSFP</span>
                </button>`;
        } else {
            const expiry = s.expiresAt
                ? new Date(s.expiresAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                : '';
            slot.innerHTML = `
                <span class="dsfp-auth-user" title="${escAttr(s.baseUrl || '')}">
                    <i data-lucide="user-check"></i>
                    <span>${escHtml(s.username || '')}</span>
                </span>
                ${expiry ? `<span class="dsfp-auth-expiry" title="Token expires">until ${escHtml(expiry)}</span>` : ''}
                <button type="button" class="dsfp-auth-btn dsfp-auth-btn-ghost" onclick="DsfpAuth.logout()">
                    <i data-lucide="log-out"></i><span>Sign out</span>
                </button>`;
        }
        if (window.lucide) lucide.createIcons();
    }

    // ---------- login modal ------------------------------------------------
    function buildModal() {
        if (state.modalEl) return state.modalEl;
        const el = document.createElement('div');
        el.id = 'dsfp-auth-modal';
        el.className = 'dsfp-modal-backdrop';
        el.style.display = 'none';
        el.innerHTML = `
            <div class="dsfp-modal" role="dialog" aria-labelledby="dsfp-auth-title" aria-modal="true">
                <div class="dsfp-modal-header">
                    <h3 id="dsfp-auth-title"><i data-lucide="shield-check"></i> Sign in to DSFP</h3>
                    <button type="button" class="dsfp-modal-close" aria-label="Close">&times;</button>
                </div>
                <form class="dsfp-modal-body" autocomplete="on">
                    <p class="dsfp-modal-hint">
                        Signs in to <strong>dsfp.norman-data.eu</strong> using your DSFP account.
                        A Bearer token (valid 1&nbsp;hour) is obtained and kept in the dashboard's
                        memory; it refreshes automatically and is used for schema lookup and all
                        sample submissions.
                    </p>
                    <div class="dsfp-form-row dsfp-form-row-2">
                        <div>
                            <label>Username</label>
                            <input type="text" name="username" autocomplete="username" required>
                        </div>
                        <div>
                            <label>Password</label>
                            <input type="password" name="password" autocomplete="current-password" required>
                        </div>
                    </div>
                    <div class="dsfp-modal-error" style="display:none;"></div>
                    <div class="dsfp-modal-footer">
                        <button type="button" class="dsfp-btn dsfp-btn-ghost" data-action="cancel">Cancel</button>
                        <button type="submit" class="dsfp-btn dsfp-btn-primary">
                            <i data-lucide="log-in"></i> Sign in
                        </button>
                    </div>
                </form>
            </div>`;
        document.body.appendChild(el);

        el.addEventListener('click', (e) => { if (e.target === el) closeModal('cancel'); });
        el.querySelector('.dsfp-modal-close').addEventListener('click', () => closeModal('cancel'));
        el.querySelector('[data-action="cancel"]').addEventListener('click', () => closeModal('cancel'));
        el.querySelector('form').addEventListener('submit', (e) => onModalSubmit(e));

        state.modalEl = el;
        if (window.lucide) lucide.createIcons();
        return el;
    }

    let modalResolver = null;

    function openLoginModal() {
        const el = buildModal();
        const form = el.querySelector('form');
        if (!form.username.value) form.username.value = (state.status && state.status.username) || '';
        form.password.value = '';
        showModalError('');
        el.style.display = 'flex';
        setTimeout(() => form.username.focus(), 30);
        return new Promise((resolve, reject) => {
            modalResolver = { resolve, reject };
        });
    }

    function closeModal(reason) {
        if (state.modalEl) state.modalEl.style.display = 'none';
        if (modalResolver) {
            const r = modalResolver;
            modalResolver = null;
            if (reason === 'success') r.resolve(state.status);
            else r.reject(new Error('Login cancelled'));
        }
    }

    function showModalError(msg) {
        if (!state.modalEl) return;
        const box = state.modalEl.querySelector('.dsfp-modal-error');
        if (!msg) { box.style.display = 'none'; box.textContent = ''; return; }
        box.textContent = msg;
        box.style.display = '';
    }

    async function onModalSubmit(e) {
        e.preventDefault();
        const form = e.currentTarget;
        const submitBtn = form.querySelector('button[type="submit"]');
        submitBtn.disabled = true;
        showModalError('');
        try {
            await DsfpAuth.login({
                username: form.username.value.trim(),
                password: form.password.value
            });
            closeModal('success');
        } catch (err) {
            showModalError(err.message || 'Login failed');
        } finally {
            submitBtn.disabled = false;
        }
    }

    // ---------- styles -----------------------------------------------------
    function injectStyles() {
        if (document.getElementById('dsfp-auth-styles')) return;
        const css = `
        .nav { display: flex; align-items: center; flex-wrap: wrap; gap: 0.25rem; }
        .nav > .nav-spacer { flex: 1 1 auto; }
        #dsfp-auth-widget {
            display: inline-flex; align-items: center; gap: 0.5rem;
            color: #ecf0f1; font-size: 0.85rem;
        }
        #dsfp-auth-widget .dsfp-auth-btn {
            background: rgba(255,255,255,0.12); color: white; border: 1px solid rgba(255,255,255,0.2);
            padding: 0.35rem 0.7rem; border-radius: 4px; cursor: pointer;
            font-family: 'Rubik', sans-serif; font-size: 0.82rem;
            display: inline-flex; align-items: center; gap: 0.35rem;
        }
        #dsfp-auth-widget .dsfp-auth-btn:hover { background: rgba(255,255,255,0.22); }
        #dsfp-auth-widget .dsfp-auth-btn-ghost { background: transparent; }
        #dsfp-auth-widget .dsfp-auth-user {
            display: inline-flex; align-items: center; gap: 0.35rem;
            background: rgba(255,255,255,0.12); padding: 0.3rem 0.6rem; border-radius: 4px;
        }
        #dsfp-auth-widget .dsfp-auth-expiry {
            opacity: 0.75; font-size: 0.75rem; font-family: 'Courier New', monospace;
        }
        #dsfp-auth-widget i { width: 14px; height: 14px; }

        .dsfp-modal-backdrop {
            position: fixed; inset: 0; background: rgba(15, 30, 45, 0.55);
            display: flex; align-items: center; justify-content: center;
            z-index: 1000; padding: 1rem;
        }
        .dsfp-modal {
            background: white; border-radius: 8px; width: 100%; max-width: 480px;
            box-shadow: 0 20px 50px rgba(0,0,0,0.3); overflow: hidden;
            font-family: 'Rubik', sans-serif; color: #2c3e50;
        }
        .dsfp-modal-header {
            background: #076196; color: white; padding: 0.9rem 1.2rem;
            display: flex; justify-content: space-between; align-items: center;
        }
        .dsfp-modal-header h3 {
            margin: 0; font-size: 1.05rem; font-weight: 500;
            display: flex; align-items: center; gap: 0.5rem;
        }
        .dsfp-modal-header i { width: 18px; height: 18px; }
        .dsfp-modal-close {
            background: transparent; color: white; border: 0; font-size: 1.4rem;
            line-height: 1; cursor: pointer; padding: 0 0.3rem;
        }
        .dsfp-modal-body { padding: 1.1rem 1.2rem 1.2rem; }
        .dsfp-modal-hint { font-size: 0.85rem; color: #5a6b78; margin-bottom: 1rem; }
        .dsfp-modal-hint-sm { font-size: 0.78rem; margin-top: 0.4rem; margin-bottom: 0; }
        .dsfp-modal-hint code {
            background: #eef3f7; padding: 0 0.3rem; border-radius: 3px;
            font-family: 'Courier New', monospace; font-size: 0.82rem;
        }
        .dsfp-form-row { margin-bottom: 0.8rem; }
        .dsfp-form-row label {
            display: block; font-size: 0.78rem; font-weight: 600;
            color: #2c3e50; margin-bottom: 0.3rem;
        }
        .dsfp-form-row input {
            width: 100%; padding: 0.55rem 0.7rem;
            border: 1px solid #dfe6ec; border-radius: 4px;
            font-family: 'Rubik', sans-serif; font-size: 0.9rem;
        }
        .dsfp-form-row input:focus { outline: 0; border-color: #3498db; box-shadow: 0 0 0 2px rgba(52,152,219,0.2); }
        .dsfp-form-row-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 0.7rem; }
        .dsfp-form-advanced { margin-bottom: 0.8rem; }
        .dsfp-form-advanced summary {
            cursor: pointer; font-size: 0.82rem; color: #094d77;
            padding: 0.35rem 0; user-select: none;
        }
        .dsfp-modal-error {
            background: #fadbd8; color: #922b21; border-left: 4px solid #e74c3c;
            padding: 0.55rem 0.8rem; border-radius: 4px;
            font-size: 0.85rem; margin-bottom: 0.7rem; white-space: pre-wrap;
        }
        .dsfp-modal-footer {
            display: flex; justify-content: flex-end; gap: 0.5rem; margin-top: 0.6rem;
        }
        .dsfp-btn {
            border: 0; border-radius: 4px; padding: 0.55rem 1rem;
            font-family: 'Rubik', sans-serif; font-size: 0.88rem; cursor: pointer;
            display: inline-flex; align-items: center; gap: 0.4rem;
        }
        .dsfp-btn-primary { background: #3498db; color: white; }
        .dsfp-btn-primary:hover { background: #2980b9; }
        .dsfp-btn-primary:disabled { background: #95c4e0; cursor: not-allowed; }
        .dsfp-btn-ghost { background: #ecf0f1; color: #2c3e50; }
        .dsfp-btn-ghost:hover { background: #dde4e6; }
        .dsfp-btn i { width: 14px; height: 14px; }

        @media (max-width: 640px) {
            .dsfp-form-row-2 { grid-template-columns: 1fr; }
            .nav > .nav-spacer { flex-basis: 100%; height: 0; }
        }`;
        const styleEl = document.createElement('style');
        styleEl.id = 'dsfp-auth-styles';
        styleEl.textContent = css;
        document.head.appendChild(styleEl);
    }

    // ---------- helpers ----------------------------------------------------
    function escHtml(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }
    function escAttr(s) { return escHtml(s); }

    // ---------- bootstrap --------------------------------------------------
    function boot() {
        if (state.booted) return;
        state.booted = true;
        injectStyles();

        // The shared header is injected asynchronously via fetch() in each page,
        // so the widget slot may not exist yet. Watch the DOM for it and render
        // as soon as it appears.
        const observer = new MutationObserver(() => {
            const slot = document.getElementById('dsfp-auth-widget');
            if (slot && slot.childNodes.length === 0) renderWidget();
        });
        observer.observe(document.documentElement, { childList: true, subtree: true });

        refresh();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', boot);
    } else {
        boot();
    }
})();
