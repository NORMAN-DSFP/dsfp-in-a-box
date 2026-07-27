/* ===========================================================================
 * DSFP bulk sample import — client logic
 * ---------------------------------------------------------------------------
 * Builds an Excel-like sheet of samples for a chosen environmental matrix,
 * attaches measurement files, and submits each sample to the live DSFP webform
 * API (via the dashboard's /api/webform/* proxy endpoints).
 *
 * Flow:
 *   1. Page loads with NO schema and NO columns.
 *   2. The moment the dashboard's DSFP session is established, the live webform
 *      schema is fetched silently from /api/webform/schema (no built-in fallback).
 *   3. The matrix selector is populated with a FLATTENED list of choices:
 *        - each env_monitoring sub-matrix (Surface water, Groundwater, …)
 *        - plus each top-level matrix that has no env sub-matrix (Human
 *          biomonitoring, Foodomics, …)
 *   4. Picking a matrix loads only the fields that are visible for it (matrix
 *      condition + env_monitoring condition + the always-applicable common
 *      group) plus the file inputs in the Files panel.
 * ======================================================================== */

// Webform file-upload fields (from the sample webform).
const FILE_FIELDS = [
    { name: 'full_scan_file_upload', label: 'Full scan' },
    { name: 'data_dependent_file_upload', label: 'Data dependent (DDA)' },
    { name: 'data_independent_file_upload', label: 'Data independent (DIA)' },
    { name: 'files_per_collision_channel', label: 'Per collision channel' }
];

// ---- Application state -----------------------------------------------------
let schema = null;           // populated from /api/webform/schema, never built-in
let scopeOptions = [];       // flattened matrix picker: [{value:'env:sw'|'matrix:foo', label, matrixKey, envKey}]
let scope = null;            // the currently picked scope option, or null
let columns = [];            // field defs currently shown as sheet columns
let rows = [];               // [{ values:{field:val}, status, message, submissionId }]
let files = [];              // [{ id, file, fieldType, assignedRow }]
let fileSeq = 0;
let schemaLoading = false;
let collectionsLoading = false;

// `collection` and `instrument_setup` are DSFP-specific reference fields whose
// options come from the live site (DKAN dataset list + per-collection CSV) and
// are loaded after sign-in instead of from the webform YAML.
let collections = [];        // [{ nid, uuid, title }] authored by the logged-in user
let setupsByNid = {};        // { nid: { loading, error, header, setups:[{...csv row}] } }
const SPECIAL_FIELDS = new Set(['collection', 'instrument_setup']);

// Admin author-override state.
let isAdmin = false;
let authorUser = null;       // { uid, uuid, name } or null

// =====================================================================
// Admin author-override
// =====================================================================
function initAdminOwnerPicker() {
    if (window.DsfpAuth) {
        DsfpAuth.on(s => {
            syncAdminOwnerPanel(s);
        });
        syncAdminOwnerPanel(DsfpAuth.getStatus());
    }
}

function syncAdminOwnerPanel(status) {
    isAdmin = !!(status && status.loggedIn && status.isAdmin);
    const panel = document.getElementById('ownerOverridePanel');
    if (panel) panel.style.display = isAdmin ? 'flex' : 'none';
    if (!isAdmin) {
        authorUser = null;
        const input = document.getElementById('ownerSearch');
        const dropdown = document.getElementById('ownerDropdown');
        const display = document.getElementById('ownerDisplay');
        if (input) input.value = '';
        if (dropdown) { dropdown.innerHTML = ''; dropdown.style.display = 'none'; }
        if (display) display.textContent = '';
    }
}

async function searchOwnerUsers() {
    const input = document.getElementById('ownerSearch');
    const dropdown = document.getElementById('ownerDropdown');
    if (!input || !dropdown) return;
    const q = input.value.trim();
    if (q.length < 2) { dropdown.innerHTML = ''; dropdown.style.display = 'none'; return; }
    try {
        const r = await fetch('/api/dsfp/users?q=' + encodeURIComponent(q));
        const users = await r.json();
        if (!Array.isArray(users) || users.length === 0) {
            dropdown.innerHTML = '<div class="owner-option owner-none">No users found</div>';
        } else {
            dropdown.innerHTML = users.map(u =>
                `<div class="owner-option" data-uuid="${escAttr(u.uuid)}" data-name="${escAttr(u.name)}" data-uid="${u.uid || ''}" onclick="selectOwnerUser(this)">${escHtml(u.name)} <span class="owner-uid">(uid ${u.uid})</span></div>`
            ).join('');
        }
        dropdown.style.display = 'block';
    } catch (e) {
        dropdown.innerHTML = '<div class="owner-option owner-none">Error: ' + escHtml(e.message) + '</div>';
        dropdown.style.display = 'block';
    }
}

function selectOwnerUser(el) {
    authorUser = { uuid: el.dataset.uuid, name: el.dataset.name, uid: el.dataset.uid };
    const input = document.getElementById('ownerSearch');
    const dropdown = document.getElementById('ownerDropdown');
    const display = document.getElementById('ownerDisplay');
    if (input) input.value = '';
    if (dropdown) { dropdown.innerHTML = ''; dropdown.style.display = 'none'; }
    if (display) display.textContent = authorUser.name + ' (uid ' + authorUser.uid + ')';
}

function clearOwnerUser() {
    authorUser = null;
    const display = document.getElementById('ownerDisplay');
    if (display) display.textContent = '';
}

function escAttr(s) { return String(s).replace(/&/g,'&amp;').replace(/"/g,'&quot;'); }
function escHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

// =====================================================================
// Schema loading (live only — no built-in fallback)
// =====================================================================
async function loadSchema() {
    if (schemaLoading) return;
    schemaLoading = true;
    const errBox = document.getElementById('connError');
    if (errBox) { errBox.textContent = ''; errBox.classList.remove('visible'); }
    setSchemaStatus('Loading schema from dsfp.norman-data.eu …');
    try {
        if (window.DsfpAuth) {
            try { await DsfpAuth.ensureLogin(); }
            catch (e) {
                setSchemaStatus('Sign in to DSFP to load the live webform schema.');
                schema = null; scopeOptions = []; scope = null;
                refreshScopeSelect();
                rebuildColumns();
                return;
            }
        }
        const resp = await fetch('/api/webform/schema', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ webformId: 'sample' })
        });
        const data = await resp.json();
        if (resp.status === 401) {
            if (errBox) { errBox.textContent = 'DSFP session expired. Please sign in again.'; errBox.classList.add('visible'); }
            setSchemaStatus('Not signed in.');
            if (window.DsfpAuth) DsfpAuth.refresh();
            schema = null; scopeOptions = []; scope = null;
            refreshScopeSelect();
            rebuildColumns();
            return;
        }
        if (!data.success) {
            if (errBox) { errBox.textContent = data.error || 'Schema unavailable'; errBox.classList.add('visible'); }
            setSchemaStatus('Could not load schema.');
            schema = null; scopeOptions = []; scope = null;
            refreshScopeSelect();
            rebuildColumns();
            return;
        }
        schema = normaliseSchema(data.schema);
        scopeOptions = buildScopeOptions(schema);
        const src = data.source === 'jsonapi' ? 'JSON:API' : 'webform_jsonschema';
        setSchemaStatus(`Schema loaded via ${src} — pick a matrix to begin.`);
        refreshScopeSelect();
        rebuildColumns();
    } catch (e) {
        if (errBox) { errBox.textContent = 'Could not load live schema: ' + e.message; errBox.classList.add('visible'); }
        setSchemaStatus('Could not load schema.');
        schema = null; scopeOptions = []; scope = null;
        refreshScopeSelect();
        rebuildColumns();
    } finally {
        schemaLoading = false;
    }
}

// Normalise a schema object returned by the server. Matrix/env options become
// {value,label} pairs; fields keep whatever group the server-side YAML parser
// assigned via webform #states conditions.
function normaliseSchema(raw) {
    if (!raw || typeof raw !== 'object') return null;
    const toPairs = arr => (arr || []).map(o =>
        typeof o === 'string' ? { value: o, label: o } : { value: o.value, label: o.label || o.value });

    const matrixOptions = toPairs(raw.matrixOptions);
    const envOptions    = toPairs(raw.envOptions);

    // The matrix key under which env_monitoring lives (the server records it
    // when it sees env_monitoring nested inside a #states.visible bound to a
    // matrix value). Fallback: any matrix value whose label/value matches
    // /environmental|env_?monitor/i.
    let envMatrixKey = raw.envMatrixKey || null;
    if (!envMatrixKey) {
        const m = matrixOptions.find(o => /environmental|env_?monitor/i.test(o.value + ' ' + o.label));
        if (m) envMatrixKey = m.value;
    }

    const fileFieldNames = new Set(FILE_FIELDS.map(f => f.name));
    // i_have_files_per_collision_channel is skipped — its value is derived
    // automatically at upload time from whether the user assigned any
    // 'Per collision channel' files to the row. `collection` and
    // `instrument_setup` ARE shown (with special-cased reference dropdowns).
    const skip = new Set([
        'matrix', 'env_monitoring', 'webform_id',
        'i_have_files_per_collision_channel',
        ...fileFieldNames
    ]);

    const fields = (raw.fields || [])
        .filter(f => !skip.has(f.name))
        .map(f => ({
            ...f,
            label: f.label || f.name,
            options: f.options
                ? f.options.map(o => typeof o === 'string' ? { value: o, label: o } : o)
                : null,
            group: f.group || (f.conditionParent === 'matrix' && f.conditionValue
                ? 'matrix:' + f.conditionValue
                : f.conditionParent === 'env_monitoring' && f.conditionValue
                    ? 'env:' + f.conditionValue
                    : 'common')
        }));

    return { matrixOptions, envOptions, envMatrixKey, fields };
}

// Build the flattened matrix picker. Each entry encodes both the top-level
// matrix and (if applicable) the env_monitoring sub-matrix.
function buildScopeOptions(s) {
    if (!s) return [];
    const env = s.envMatrixKey;
    const matrixLabel = (v) => {
        const m = s.matrixOptions.find(o => o.value === v);
        return m ? m.label : v;
    };
    const list = [];
    // Env sub-matrices first (Surface water, Groundwater, …) — they're the
    // common case for environmental monitoring submissions.
    if (env) {
        s.envOptions.forEach(o => list.push({
            value: 'env:' + o.value,
            label: o.label,
            matrixKey: env,
            envKey: o.value
        }));
    }
    // Then every matrix option that doesn't have env sub-options
    // (Human biomonitoring, Foodomics, …).
    s.matrixOptions.forEach(o => {
        if (o.value === env) return;
        list.push({
            value: 'matrix:' + o.value,
            label: o.label,
            matrixKey: o.value,
            envKey: ''
        });
    });
    return list;
}

// =====================================================================
// Matrix selection + columns
// =====================================================================

// (Re)populate the matrix dropdown with the current scopeOptions and restore
// the prior selection if it still exists.
function refreshScopeSelect() {
    const sel = document.getElementById('matrixSelect');
    if (!sel) return;
    const prev = scope ? scope.value : '';
    if (scopeOptions.length === 0) {
        sel.innerHTML = `<option value="">— Sign in to load matrices —</option>`;
        sel.disabled = true;
        return;
    }
    sel.disabled = false;
    sel.innerHTML = `<option value="">— Select a matrix —</option>` +
        scopeOptions.map(o => `<option value="${escAttr(o.value)}">${escHtml(o.label)}</option>`).join('');
    if (prev && scopeOptions.find(o => o.value === prev)) sel.value = prev;
}

function onMatrixChange() {
    const v = val('matrixSelect');
    scope = scopeOptions.find(o => o.value === v) || null;
    // The env_monitoring group / dropdown from the old two-step UI is gone;
    // hide it in case the HTML still has the element.
    const envGroup = document.getElementById('envMonitoringGroup');
    if (envGroup) envGroup.style.display = 'none';
    rebuildColumns();
}

function currentMatrix() { return scope ? scope.matrixKey : ''; }
function currentEnv()    { return scope ? scope.envKey    : ''; }

// A field is visible only when its webform #states condition matches the
// selected scope. Fields with no condition (group === 'common') are always
// shown once a matrix is selected.
function fieldApplies(f) {
    if (!scope) return false;
    if (f.group === 'common') return true;
    if (f.group.startsWith('matrix:')) {
        // matrix-conditional: matches the scope's matrixKey, AND when env exists
        // the matrix-condition is the env's parent matrix (so env sub-matrices
        // inherit common matrix-level fields).
        return f.group.slice(7) === scope.matrixKey;
    }
    if (f.group.startsWith('env:')) {
        return scope.envKey && f.group.slice(4) === scope.envKey;
    }
    return false;
}

function rebuildColumns() {
    columns = schema ? schema.fields.filter(fieldApplies) : [];
    renderSheet();
    renderFileList();
}

// =====================================================================
// Spreadsheet
// =====================================================================

// Build a short, human-readable summary of a field's validation constraints
// (shown beneath the machine name in the column header) and a long tooltip.
function fieldRulesText(f) {
    const bits = [];
    if (f.options && f.options.length) bits.push(`enum (${f.options.length})`);
    if (isTaxonomyField(f)) bits.push('taxonomy: ' + (f.targetBundles || []).join(','));
    if (f.type === 'date' || f.type === 'webform_datelist') bits.push('date');
    else if (f.type === 'email') bits.push('email');
    else if (f.type === 'tel') bits.push('phone');
    else if (f.type === 'url') bits.push('url');
    else if (f.type === 'number') bits.push('number');
    if (typeof f.min === 'number') bits.push('≥' + f.min);
    if (typeof f.max === 'number') bits.push('≤' + f.max);
    if (typeof f.minlength === 'number') bits.push('min ' + f.minlength + 'ch');
    if (typeof f.maxlength === 'number') bits.push('max ' + f.maxlength + 'ch');
    if (f.pattern) bits.push('pattern');
    return bits.join(' · ');
}

// True when the field is a taxonomy-term entity reference whose vocabularies
// we can query live for autocomplete suggestions.
function isTaxonomyField(f) {
    return f && f.targetType === 'taxonomy_term'
        && Array.isArray(f.targetBundles) && f.targetBundles.length > 0;
}

function fieldTooltip(f) {
    const lines = [];
    if (f.description) lines.push(stripTags(f.description));
    if (f.required) lines.push('Required');
    if (f.type) lines.push('Type: ' + f.type);
    if (f.options && f.options.length) {
        const sample = f.options.slice(0, 12).map(o => o.label || o.value).join(', ');
        lines.push('Allowed: ' + sample + (f.options.length > 12 ? `, … (${f.options.length} total)` : ''));
    }
    if (typeof f.min === 'number') lines.push('Min value: ' + f.min);
    if (typeof f.max === 'number') lines.push('Max value: ' + f.max);
    if (typeof f.minlength === 'number') lines.push('Min length: ' + f.minlength);
    if (typeof f.maxlength === 'number') lines.push('Max length: ' + f.maxlength);
    if (f.pattern) lines.push('Pattern: ' + f.pattern);
    return lines.join('\n');
}

function stripTags(s) { return String(s || '').replace(/<[^>]+>/g, '').trim(); }

// Validate a single cell value against the field's webform-derived rules.
// Returns a short error message or null when valid. Blank values are accepted
// here (required-ness is enforced at upload time so partially-filled rows
// don't show angry red borders while still being edited).
function validateCell(value, f) {
    const v = (value == null ? '' : String(value)).trim();
    if (v === '') return null;
    // Taxonomy autocomplete values are validated server-side at submit time;
    // the live set of acceptable terms is too big and dynamic to enumerate here.
    if (isTaxonomyField(f)) return null;
    // collection / instrument_setup get their valid IDs from live API calls;
    // the dropdown itself constrains the choice.
    if (SPECIAL_FIELDS.has(f.name)) return null;
    if (f.options && f.options.length) {
        const allowed = new Set(f.options.map(o => String(o.value)));
        const allowedLabels = new Set(f.options.map(o => String(o.label)));
        if (!allowed.has(v) && !allowedLabels.has(v)) return 'Not in allowed list';
    }
    if (f.type === 'email' && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v)) return 'Invalid email';
    if (f.type === 'url'   && !/^https?:\/\/\S+$/.test(v))           return 'Invalid URL';
    if (f.type === 'date' || f.type === 'webform_datelist') {
        if (isNaN(Date.parse(v))) return 'Invalid date (use YYYY-MM-DD)';
    }
    if (f.type === 'number') {
        const n = Number(v);
        if (!Number.isFinite(n)) return 'Must be a number';
        if (typeof f.min === 'number' && n < f.min) return 'Must be ≥ ' + f.min;
        if (typeof f.max === 'number' && n > f.max) return 'Must be ≤ ' + f.max;
    } else {
        if (typeof f.minlength === 'number' && v.length < f.minlength)
            return `Min length ${f.minlength}`;
        if (typeof f.maxlength === 'number' && v.length > f.maxlength)
            return `Max length ${f.maxlength}`;
        if (f.pattern) {
            try {
                if (!new RegExp('^(?:' + f.pattern + ')$').test(v))
                    return f.patternError || 'Does not match required pattern';
            } catch (_) { /* invalid regex from server — ignore */ }
        }
    }
    return null;
}

function renderSheet() {
    const head = document.getElementById('sheetHead');
    const body = document.getElementById('sheetBody');

    // Empty state #1: schema not loaded yet.
    if (!schema) {
        head.innerHTML = '';
        body.innerHTML = `<tr><td class="sheet-empty">
            Sign in to DSFP (top-right) to load the webform schema.
        </td></tr>`;
        return;
    }
    // Empty state #2: schema loaded, but no matrix picked yet.
    if (!scope) {
        head.innerHTML = '';
        body.innerHTML = `<tr><td class="sheet-empty">
            Pick a <strong>matrix</strong> in the top bar. Only the fields
            relevant to that matrix — plus the common metadata and the file
            attachments — will be loaded.
        </td></tr>`;
        return;
    }

    head.innerHTML =
        `<th class="row-head">#</th>` +
        columns.map(c => {
            const rules = fieldRulesText(c);
            const tip = fieldTooltip(c);
            return `
            <th class="${c.required ? 'req' : ''}" title="${escAttr(tip)}">
                <span class="col-label">${escHtml(c.label)}</span>
                <span class="col-machine">${escHtml(c.name)}</span>
                ${rules ? `<span class="col-rules">${escHtml(rules)}</span>` : ''}
            </th>`;
        }).join('') +
        `<th class="status-col">Files</th>`;

    if (rows.length === 0) addRows(3, true);

    body.innerHTML = rows.map((row, r) => {
        const hasFiles = files.some(f => f.assignedRow === r);
        const cells = columns.map(c => renderCell(row, r, c, hasFiles)).join('');
        return `<tr>
            <td class="row-head">${r + 1}</td>
            ${cells}
            <td class="status-col" id="status-${r}">${renderStatus(row, r)}</td>
        </tr>`;
    }).join('');

    // Attach a single paste handler to editable cells (delegated).
    body.querySelectorAll('td[contenteditable]').forEach(td => {
        td.addEventListener('paste', onCellPaste);
    });
    if (window.lucide) lucide.createIcons();
    syncSheetScrollbars();
}

// Mirror the sheet's horizontal scroll position into the dummy scrollbar
// placed above the table. Called whenever the sheet's column set or row
// count changes (which is what affects scrollWidth).
function syncSheetScrollbars() {
    const wrapper = document.getElementById('sheetWrapper');
    const top = document.getElementById('sheetScrollTop');
    const spacer = document.getElementById('sheetScrollSpacer');
    const sheet = document.getElementById('sheet');
    if (!wrapper || !top || !spacer || !sheet) return;
    spacer.style.width = sheet.scrollWidth + 'px';
    // Hide the top scrollbar when no horizontal overflow is needed.
    top.style.display = sheet.scrollWidth > wrapper.clientWidth ? '' : 'none';
}

// Render a single sheet cell. Fields with #options become a real <select>;
// taxonomy-reference fields become an <input> backed by a debounced datalist;
// boolean-ish fields become a checkbox; everything else stays an editable
// cell that supports Excel-style paste.
function renderCell(row, r, c, hasFiles) {
    const v = row.values[c.name] || '';
    const err = validateCell(v, c);
    const classes = ['cell'];
    if (hasFiles) classes.push('has-files');
    if (err) classes.push('invalid');

    // ---- DSFP-special: collection (user's DKAN datasets) ----
    if (c.name === 'collection') {
        const opts = [
            `<option value=""${!v ? ' selected' : ''}>${collections.length ? '— Select a collection —' : '— Loading… —'}</option>`,
            ...collections.map(col => {
                const isSel = String(col.nid) === String(v) ? ' selected' : '';
                return `<option value="${escAttr(col.nid)}"${isSel}>${escHtml(col.title)}</option>`;
            })
        ].join('');
        return `<td class="${classes.join(' ')} cell-select" title="${escAttr(err || fieldTooltip(c))}">
            <select data-row="${r}" data-field="${escAttr(c.name)}"
                onchange="onCollectionChange(this)">${opts}</select>
        </td>`;
    }

    // ---- DSFP-special: instrument_setup (view-restricted to the row's collection) ----
    if (c.name === 'instrument_setup') {
        const collNid = row.values['collection'] || '';
        const entry = collNid ? setupsByNid[collNid] : null;
        let placeholder, options = [], disabled = false;
        if (!collNid)            { placeholder = '— Pick a collection first —'; disabled = true; }
        else if (!entry)         { placeholder = '— Loading… —'; disabled = true; loadInstrumentSetups(collNid); }
        else if (entry.loading)  { placeholder = '— Loading… —'; disabled = true; }
        else if (entry.error)    { placeholder = '— Error: ' + entry.error + ' —'; disabled = true; }
        else if (!entry.setups || !entry.setups.length) { placeholder = '— No setups defined —'; disabled = true; }
        else                     { placeholder = '— Select an instrument setup —'; options = entry.setups; }

        const labelKey = options.length ? pickSetupLabelKey(entry.header) : null;
        const valueKey = options.length ? pickSetupValueKey(entry.header) : null;
        const opts = [
            `<option value=""${!v ? ' selected' : ''}>${escHtml(placeholder)}</option>`,
            ...options.map(s => {
                const val = String(s[valueKey] || '');
                const lbl = labelKey ? String(s[labelKey] || val) : val;
                const isSel = val === String(v) ? ' selected' : '';
                return `<option value="${escAttr(val)}"${isSel}>${escHtml(lbl)}</option>`;
            })
        ].join('');
        return `<td class="${classes.join(' ')} cell-select" title="${escAttr(err || fieldTooltip(c))}">
            <select data-row="${r}" data-field="${escAttr(c.name)}"
                ${disabled ? 'disabled' : ''} onchange="onSelectChange(this)">${opts}</select>
        </td>`;
    }

    // ---- Multi-value / single-select dropdown ----
    if (c.options && c.options.length) {
        const multi = c.multiple;
        const selected = multi
            ? new Set(String(v).split(',').map(s => s.trim()).filter(Boolean))
            : new Set([String(v)]);
        const opts = [
            `<option value=""${!v ? ' selected' : ''}>—</option>`,
            ...c.options.map(o => {
                const val = String(o.value);
                const isSel = selected.has(val) ? ' selected' : '';
                return `<option value="${escAttr(val)}"${isSel}>${escHtml(o.label)}</option>`;
            })
        ].join('');
        return `<td class="${classes.join(' ')} cell-select" title="${escAttr(err || fieldTooltip(c))}">
            <select data-row="${r}" data-field="${escAttr(c.name)}"
                ${multi ? 'multiple size="4"' : ''} onchange="onSelectChange(this)">${opts}</select>
        </td>`;
    }

    // ---- Taxonomy autocomplete ----
    if (isTaxonomyField(c)) {
        const listId = `tx-${c.name}-${r}`;
        const vocabs = (c.targetBundles || []).join(',');
        return `<td class="${classes.join(' ')} cell-tax" title="${escAttr(err || fieldTooltip(c))}">
            <input type="text" list="${listId}" value="${escAttr(v)}"
                data-row="${r}" data-field="${escAttr(c.name)}"
                data-vocab="${escAttr(vocabs)}"
                placeholder="Type to search …"
                oninput="onTaxonomyInput(this)">
            <datalist id="${listId}"></datalist>
        </td>`;
    }

    // ---- Checkbox ----
    if (c.type === 'checkbox') {
        const checked = (v === '1' || v === 'true' || v === 1 || v === true) ? ' checked' : '';
        return `<td class="${classes.join(' ')} cell-check" title="${escAttr(err || fieldTooltip(c))}">
            <input type="checkbox" data-row="${r}" data-field="${escAttr(c.name)}"${checked}
                onchange="onCheckChange(this)">
        </td>`;
    }

    // ---- Date / number / text ----
    return `<td contenteditable="true" data-row="${r}" data-field="${escAttr(c.name)}"
        class="${classes.join(' ')}"
        title="${escAttr(err || fieldTooltip(c))}"
        oninput="onCellInput(this)">${escHtml(v)}</td>`;
}

function onSelectChange(sel) {
    const r = parseInt(sel.getAttribute('data-row'), 10);
    const field = sel.getAttribute('data-field');
    if (!rows[r]) return;
    const c = columns.find(col => col.name === field);
    let v;
    if (sel.multiple) {
        v = Array.from(sel.selectedOptions).map(o => o.value).filter(Boolean).join(',');
    } else {
        v = sel.value;
    }
    rows[r].values[field] = v;
    const td = sel.closest('td');
    if (c && td) {
        const err = validateCell(v, c);
        td.classList.toggle('invalid', !!err);
        td.title = err || fieldTooltip(c);
    }
    resetRowStatusIfNeeded(r);
}

function onCheckChange(cb) {
    const r = parseInt(cb.getAttribute('data-row'), 10);
    const field = cb.getAttribute('data-field');
    if (!rows[r]) return;
    rows[r].values[field] = cb.checked ? '1' : '';
    resetRowStatusIfNeeded(r);
}

// ---- Collection + instrument-setup wiring ---------------------------------
// The user picks one of their own DKAN collections per row; the row's
// instrument_setup dropdown then loads the setups defined for that collection.
async function loadCollections() {
    if (collectionsLoading) return;
    collectionsLoading = true;
    try {
        const r = await fetch('/api/dsfp/collections');
        if (r.status === 401) {
            collections = [];
            if (window.DsfpAuth) DsfpAuth.refresh();
            return;
        }
        const data = await r.json();
        if (!data.success) {
            console.warn('[collections] could not load:', data.error);
            collections = [];
        } else {
            collections = data.collections || [];
        }
    } catch (e) {
        console.warn('[collections] fetch failed:', e.message);
        collections = [];
    } finally {
        collectionsLoading = false;
    }
    // Refresh any already-rendered "collection" cells.
    if (columns.some(c => c.name === 'collection')) renderSheet();
}

async function loadInstrumentSetups(nid) {
    if (!nid) return;
    if (setupsByNid[nid] && !setupsByNid[nid].error) return;
    setupsByNid[nid] = { loading: true };
    try {
        const r = await fetch('/api/dsfp/instrument-setups?nid=' + encodeURIComponent(nid));
        if (r.status === 401) {
            setupsByNid[nid] = { error: 'session expired', setups: [] };
            if (window.DsfpAuth) DsfpAuth.refresh();
            return;
        }
        const data = await r.json();
        if (!data.success) {
            setupsByNid[nid] = { error: data.error || 'load failed', setups: [] };
        } else {
            setupsByNid[nid] = {
                loading: false,
                header: data.header || [],
                setups: data.setups || []
            };
        }
    } catch (e) {
        setupsByNid[nid] = { error: e.message, setups: [] };
    }
    // Re-render any rows that reference this collection so their
    // instrument_setup dropdown picks up the freshly-loaded options.
    if (columns.some(c => c.name === 'instrument_setup')) renderSheet();
}

// Pick best-effort value / label columns from the CSV header. The value
// column is the submission ID expected by the webform; the label is the
// human-readable name shown in the dropdown.
function pickSetupValueKey(header) {
    if (!header || !header.length) return null;
    const prefs = ['sid', 'submission_id', 'id', 'nid'];
    for (const p of prefs) {
        const m = header.find(h => h.toLowerCase() === p);
        if (m) return m;
    }
    return header[0];
}
function pickSetupLabelKey(header) {
    if (!header || !header.length) return null;
    const prefs = ['label', 'name', 'title', 'instrument_setup_name', 'description'];
    for (const p of prefs) {
        const m = header.find(h => h.toLowerCase() === p);
        if (m) return m;
    }
    // Otherwise fall back to whichever non-id column comes first.
    const vk = pickSetupValueKey(header);
    return header.find(h => h !== vk) || vk;
}

// When the user picks a collection, refresh that row's instrument_setup cell
// (clear any stale value) and kick off loading the setups for the new nid.
function onCollectionChange(sel) {
    const r = parseInt(sel.getAttribute('data-row'), 10);
    if (!rows[r]) return;
    const newNid = sel.value;
    const prevNid = rows[r].values['collection'];
    rows[r].values['collection'] = newNid;
    if (newNid !== prevNid) {
        // Clear stale instrument_setup pick; it's view-restricted to the
        // previous collection's nid.
        rows[r].values['instrument_setup'] = '';
    }
    resetRowStatusIfNeeded(r);
    if (newNid) loadInstrumentSetups(newNid);
    renderSheet();
}

// ---- Taxonomy autocomplete ------------------------------------------------
// Each input has its own debounce timer keyed by element so simultaneous
// edits in different rows don't cancel each other's lookups. Results are
// cached per (vocab, query-prefix) to keep the live site cool.
const _taxonomyTimers = new WeakMap();
const _taxonomyCache = new Map();    // 'vocab|q' -> [{name,...}]

function onTaxonomyInput(inp) {
    const r = parseInt(inp.getAttribute('data-row'), 10);
    const field = inp.getAttribute('data-field');
    const vocab = inp.getAttribute('data-vocab') || '';
    if (!rows[r]) return;
    rows[r].values[field] = inp.value;

    const c = columns.find(col => col.name === field);
    const td = inp.closest('td');
    if (c && td) {
        const err = validateCell(inp.value, c);
        td.classList.toggle('invalid', !!err);
        td.title = err || fieldTooltip(c);
    }
    resetRowStatusIfNeeded(r);

    const q = inp.value.trim();
    if (q.length < 2) return;            // don't hammer the API on single chars

    clearTimeout(_taxonomyTimers.get(inp));
    const t = setTimeout(() => fetchTaxonomySuggestions(inp, vocab, q), 250);
    _taxonomyTimers.set(inp, t);
}

async function fetchTaxonomySuggestions(inp, vocab, q) {
    const key = vocab + '|' + q.toLowerCase();
    let terms = _taxonomyCache.get(key);
    if (!terms) {
        try {
            const url = `/api/dsfp/taxonomy?vocab=${encodeURIComponent(vocab)}&q=${encodeURIComponent(q)}`;
            const resp = await fetch(url);
            if (!resp.ok) return;
            const data = await resp.json();
            terms = (data && data.success && data.terms) || [];
            _taxonomyCache.set(key, terms);
        } catch (_) { return; }
    }
    const listId = inp.getAttribute('list');
    const list = listId && document.getElementById(listId);
    if (!list) return;
    list.innerHTML = terms.slice(0, 25).map(t =>
        `<option value="${escAttr(t.name)}">${escHtml(t.vocab || '')}</option>`
    ).join('');
}

function resetRowStatusIfNeeded(r) {
    if (rows[r].status === 'success' || rows[r].status === 'failed') {
        rows[r].status = 'pending';
        const cell = document.getElementById('status-' + r);
        if (cell) cell.innerHTML = renderStatus(rows[r], r);
    }
}

function renderStatus(row, r) {
    const fileCount = (typeof r === 'number')
        ? files.filter(f => f.assignedRow === r).length
        : 0;
    const text = fileCount === 0
        ? 'No files'
        : `${fileCount} file${fileCount === 1 ? '' : 's'}`;
    const cls = fileCount === 0 ? 's-pending' : 's-success';
    return `<div class="cell-status"><span class="status-badge ${cls}">${text}</span></div>`;
}

function onCellInput(td) {
    const r = parseInt(td.getAttribute('data-row'), 10);
    const field = td.getAttribute('data-field');
    if (!rows[r]) return;
    rows[r].values[field] = td.textContent;

    // Live validate this cell only — cheap and avoids a full re-render that
    // would steal focus while the user types.
    const f = columns.find(c => c.name === field);
    if (f) {
        const err = validateCell(td.textContent, f);
        td.classList.toggle('invalid', !!err);
        td.title = err || fieldTooltip(f);
    }

    resetRowStatusIfNeeded(r);
}

function onCellPaste(e) {
    const text = (e.clipboardData || window.clipboardData).getData('text');
    if (!text) return;
    e.preventDefault();

    const td = e.currentTarget;
    const startRow = parseInt(td.getAttribute('data-row'), 10);
    const startField = td.getAttribute('data-field');
    const startCol = columns.findIndex(c => c.name === startField);

    const lines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
    while (lines.length && lines[lines.length - 1] === '') lines.pop();
    const matrix = lines.map(l => l.split('\t'));

    // Ensure enough rows exist.
    const needed = startRow + matrix.length;
    while (rows.length < needed) rows.push(newRow());

    matrix.forEach((cells, i) => {
        cells.forEach((cellVal, j) => {
            const colIdx = startCol + j;
            if (colIdx < 0 || colIdx >= columns.length) return;
            rows[startRow + i].values[columns[colIdx].name] = cellVal;
        });
    });

    renderSheet();
    // Restore focus near the paste origin.
    const restore = document.querySelector(`td[data-row="${startRow}"][data-field="${cssAttr(startField)}"]`);
    if (restore) restore.focus();
}

function newRow() {
    return { values: {}, status: 'pending', message: '', submissionId: null };
}

function addRows(n, skipRender) {
    // Only allow adding rows once a matrix is selected — otherwise the sheet
    // has no columns to put the data in.
    if (!scope) {
        if (!skipRender) alert('Select a matrix first.');
        return;
    }
    for (let i = 0; i < n; i++) rows.push(newRow());
    if (!skipRender) renderSheet();
}

function clearSheet() {
    if (!confirm('Clear all samples from the sheet?')) return;
    rows = [];
    renderSheet();
}

// =====================================================================
// Files
// =====================================================================
function onFilesSelected(e) {
    addFiles(e.target.files);
    e.target.value = '';
}

function addFiles(fileList) {
    Array.from(fileList).forEach(file => {
        files.push({
            id: ++fileSeq,
            file,
            fieldType: guessFieldType(file.name),
            assignedRow: null,
            // Only meaningful for files_per_collision_channel; the table
            // disables the input for any other field type.
            collisionEnergy: ''
        });
    });
    renderFileList();
}

function guessFieldType(name) {
    const n = name.toLowerCase();
    if (/(dia|independent|swath)/.test(n)) return 'data_independent_file_upload';
    if (/(dda|dependent)/.test(n)) return 'data_dependent_file_upload';
    if (/(collision|channel|mrm|prm)/.test(n)) return 'files_per_collision_channel';
    return 'full_scan_file_upload';
}

function renderFileList() {
    const container = document.getElementById('fileListContainer');
    if (files.length === 0) {
        container.innerHTML = '<p class="muted" style="margin-top:1rem;">No files added yet.</p>';
        return;
    }
    const rowOptions = rows.map((row, i) => {
        const sn = row.values['short_name_for_contribution'] || '';
        const label = sn ? `${i + 1}: ${sn}` : `Row ${i + 1}`;
        return { i, label };
    });

    container.innerHTML = `
        <table class="file-table">
            <thead><tr>
                <th>File</th><th>Size</th><th>Field type</th>
                <th title="Active only for 'Per collision channel' files">Collision energy</th>
                <th>Assigned sample</th><th></th>
            </tr></thead>
            <tbody>
            ${files.map(f => {
                const matched = f.assignedRow !== null && f.assignedRow !== undefined;
                const perChannel = f.fieldType === 'files_per_collision_channel';
                return `<tr>
                    <td class="file-name ${matched ? 'matched' : 'unmatched'}">${escHtml(f.file.name)}</td>
                    <td class="muted">${formatSize(f.file.size)}</td>
                    <td>
                        <select onchange="setFileField(${f.id}, this.value)">
                            ${FILE_FIELDS.map(ff => `<option value="${ff.name}" ${f.fieldType === ff.name ? 'selected' : ''}>${escHtml(ff.label)}</option>`).join('')}
                        </select>
                    </td>
                    <td>
                        <input type="text"
                            value="${escAttr(f.collisionEnergy || '')}"
                            placeholder="${perChannel ? 'e.g. 20 eV' : '—'}"
                            ${perChannel ? '' : 'disabled'}
                            style="width: 90px;"
                            oninput="setFileEnergy(${f.id}, this.value)">
                    </td>
                    <td>
                        <select onchange="setFileRow(${f.id}, this.value)">
                            <option value="">— unassigned —</option>
                            ${rowOptions.map(o => `<option value="${o.i}" ${f.assignedRow === o.i ? 'selected' : ''}>${escHtml(o.label)}</option>`).join('')}
                        </select>
                    </td>
                    <td><button class="btn btn-danger" style="padding:0.25rem 0.5rem;" onclick="removeFile(${f.id})"><i data-lucide="x" class="icon-sm"></i></button></td>
                </tr>`;
            }).join('')}
            </tbody>
        </table>`;
    if (window.lucide) lucide.createIcons();
}

function setFileField(id, value) {
    const f = files.find(x => x.id === id);
    if (!f) return;
    f.fieldType = value;
    // Clear stale energy when switching away from per-channel.
    if (value !== 'files_per_collision_channel') f.collisionEnergy = '';
    renderFileList();
}

function setFileEnergy(id, value) {
    const f = files.find(x => x.id === id);
    if (f) f.collisionEnergy = value;
}

function setFileRow(id, value) {
    const f = files.find(x => x.id === id);
    if (!f) return;
    f.assignedRow = value === '' ? null : parseInt(value, 10);
    renderSheet();
    renderFileList();
}

function removeFile(id) {
    files = files.filter(x => x.id !== id);
    renderSheet();
    renderFileList();
}

function clearFiles() {
    files = [];
    renderSheet();
    renderFileList();
}

// Match each file to a sample by comparing the base file name to the
// short_name_for_contribution column value.
function autoMatchFiles() {
    let matched = 0;
    files.forEach(f => {
        const base = stripExt(f.file.name).toLowerCase().trim();
        const idx = rows.findIndex(r => {
            const sn = (r.values['short_name_for_contribution'] || '').toLowerCase().trim();
            return sn && sn === base;
        });
        if (idx !== -1) { f.assignedRow = idx; matched++; }
    });
    renderSheet();
    renderFileList();
    setSchemaStatus(`Auto-matched ${matched} of ${files.length} file(s).`);
}

// =====================================================================
// Upload
// =====================================================================
async function uploadAll() {
    const webformId = 'sample';

    if (window.DsfpAuth) {
        try { await DsfpAuth.ensureLogin(); }
        catch (e) { alert('Please sign in to DSFP first.'); return; }
    }

    const toUpload = rows
        .map((row, idx) => ({ row, idx }))
        .filter(({ row }) => Object.values(row.values).some(v => (v || '').toString().trim() !== ''));

    if (toUpload.length === 0) { alert('No sample rows to upload.'); return; }

    // ---- Client-side validation gate -----------------------------------
    // Stop the user before we hammer the server with rows we already know
    // will fail webform constraints (required, enum, regex, range …).
    const issues = [];
    toUpload.forEach(({ row, idx }) => {
        columns.forEach(c => {
            const v = (row.values[c.name] || '').toString().trim();
            if (c.required && v === '') {
                issues.push(`Row ${idx + 1}: "${c.label}" (${c.name}) is required.`);
                return;
            }
            const err = validateCell(v, c);
            if (err) issues.push(`Row ${idx + 1}: "${c.label}" (${c.name}) — ${err}.`);
        });
    });
    if (issues.length) {
        const head = issues.slice(0, 12).join('\n');
        const more = issues.length > 12 ? `\n\n… and ${issues.length - 12} more issue(s).` : '';
        alert(`Cannot upload — fix these issues first:\n\n${head}${more}`);
        return;
    }

    const btn = document.getElementById('uploadBtn');
    btn.disabled = true;
    let done = 0, ok = 0, fail = 0;

    const matrix = currentMatrix();
    const env = currentEnv();

    for (const { row, idx } of toUpload) {
        row.status = 'uploading';
        updateStatusCell(idx);

        try {
            const fd = new FormData();
            fd.append('webform_id', webformId);

            const assignedFiles = files.filter(f => f.assignedRow === idx);
            const perChannelFiles = assignedFiles.filter(f => f.fieldType === 'files_per_collision_channel');

            const fieldValues = Object.assign({}, row.values);
            fieldValues.matrix = matrix;
            if (env) fieldValues.env_monitoring = env;
            // Derived automatically from the assigned files \u2014 the dedicated
            // sample-table column for this boolean has been removed.
            fieldValues.i_have_files_per_collision_channel = perChannelFiles.length > 0 ? '1' : '0';
            if (perChannelFiles.length) {
                fieldValues.collision_energies = perChannelFiles.map(f => f.collisionEnergy || '');
            }
            fd.append('fields', JSON.stringify(fieldValues));

            if (authorUser) fd.append('author_uuid', authorUser.uuid);

            assignedFiles.forEach(f => fd.append(f.fieldType, f.file, f.file.name));

            const resp = await fetch('/api/webform/import', { method: 'POST', body: fd });
            const data = await resp.json().catch(() => ({}));

            if (resp.status === 401) {
                row.status = 'failed';
                row.message = 'Session expired. Please sign in again.';
                fail++;
                if (window.DsfpAuth) DsfpAuth.refresh();
                updateStatusCell(idx);
                done++;
                updateProgress(done, toUpload.length, ok, fail);
                break;
            }

            if (data.success) {
                row.status = 'success';
                row.submissionId = data.submission_id || (data.response && data.response.submission_id) || null;
                ok++;
            } else {
                row.status = 'failed';
                row.message = describeError(data);
                fail++;
            }
        } catch (e) {
            row.status = 'failed';
            row.message = e.message;
            fail++;
        }

        updateStatusCell(idx);
        done++;
        updateProgress(done, toUpload.length, ok, fail);
    }

    btn.disabled = false;
}

function describeError(data) {
    if (!data) return 'Unknown error';
    if (data.error) return data.error;
    const r = data.response;
    if (r && typeof r === 'object') {
        if (r.validation_errors) return 'Validation: ' + Object.values(r.validation_errors).join('; ');
        if (r.message) return r.message;
        if (r.error) return r.error;
    }
    if (typeof r === 'string') return truncate(r, 200);
    return `HTTP ${data.status || '?'}`;
}

function updateStatusCell(idx) {
    const cell = document.getElementById('status-' + idx);
    if (cell) cell.innerHTML = renderStatus(rows[idx], idx);
}

function updateProgress(done, total, ok, fail) {
    document.getElementById('progressFill').style.width = Math.round((done / total) * 100) + '%';
    document.getElementById('progressText').textContent =
        `${done} / ${total} processed — ${ok} submitted, ${fail} failed.`;
}

// =====================================================================
// Helpers
// =====================================================================
function val(id) { return (document.getElementById(id).value || '').trim(); }
function setSchemaStatus(msg) { document.getElementById('schemaStatus').textContent = msg; }
function stripExt(name) { return name.replace(/\.[^.]+$/, ''); }
function truncate(s, n) { return s && s.length > n ? s.slice(0, n) + '…' : s; }
function cssAttr(s) { return s.replace(/"/g, '\\"'); }

function formatSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
}

function escHtml(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function escAttr(s) { return escHtml(s); }

// ---- Drag & drop wiring ----------------------------------------------------
(function initDropzone() {
    const dz = document.getElementById('dropzone');
    if (!dz) return;
    ['dragenter', 'dragover'].forEach(ev =>
        dz.addEventListener(ev, e => { e.preventDefault(); dz.classList.add('dragover'); }));
    ['dragleave', 'drop'].forEach(ev =>
        dz.addEventListener(ev, e => { e.preventDefault(); dz.classList.remove('dragover'); }));
    dz.addEventListener('drop', e => {
        if (e.dataTransfer && e.dataTransfer.files) addFiles(e.dataTransfer.files);
    });
})();

// ---- Bootstrap -------------------------------------------------------------
// DSFP session status is shown by the dashboard-wide auth widget in the nav.
// We render the empty state immediately, then trigger schema loading the
// moment the dashboard reports a logged-in session.
document.addEventListener('DOMContentLoaded', () => {
    refreshScopeSelect();
    renderSheet();
    renderFileList();
    initAdminOwnerPicker();

    // Two-way sync between the dummy top scrollbar and the real sheet wrapper.
    const top = document.getElementById('sheetScrollTop');
    const wrapper = document.getElementById('sheetWrapper');
    if (top && wrapper) {
        let lock = false;
        top.addEventListener('scroll', () => {
            if (lock) return; lock = true;
            wrapper.scrollLeft = top.scrollLeft;
            lock = false;
        });
        wrapper.addEventListener('scroll', () => {
            if (lock) return; lock = true;
            top.scrollLeft = wrapper.scrollLeft;
            lock = false;
        });
        window.addEventListener('resize', syncSheetScrollbars);
    }

    if (window.DsfpAuth) {
        DsfpAuth.on(async (status) => {
            if (status && status.loggedIn) {
                if (!schema && !schemaLoading) await loadSchema();
                if (!collections.length && !collectionsLoading) loadCollections();
            } else if (status && !status.loggedIn) {
                schema = null; scopeOptions = []; scope = null;
                columns = [];
                collections = [];
                setupsByNid = {};
                refreshScopeSelect();
                renderSheet();
                renderFileList();
                setSchemaStatus('Sign in to DSFP to load the live webform schema.');
            }
        });
    }
});
