// Generic click-to-sort behaviour for tables marked with data-sortable="true".
//
// Usage: add `data-sortable="true"` to any <table> element (static markup or
// server/JS-rendered HTML string). This script uses a single delegated click
// listener attached to `document`, so it keeps working even when a table's
// innerHTML (including the <table> tag itself) is regenerated dynamically -
// there is no need to "re-initialize" sorting after each render.
//
// Sorting reorders the existing <tr> elements in place (it does not clone or
// rebuild them), so any event listeners already bound to cells/buttons in a
// row are preserved.
(function () {
    function getHeaderRow(table) {
        return table.rows.length ? table.rows[0] : null;
    }

    function cellText(row, index) {
        const cell = row.cells[index];
        return cell ? cell.textContent.trim() : '';
    }

    function compareValues(a, b) {
        const na = parseFloat(a.replace(/[,%]/g, ''));
        const nb = parseFloat(b.replace(/[,%]/g, ''));
        const looksNumeric = (s) => s === '' || /^-?[\d,.]+%?$/.test(s);
        if (looksNumeric(a) && looksNumeric(b) && !isNaN(na) && !isNaN(nb)) {
            return na - nb;
        }
        const da = Date.parse(a);
        const db = Date.parse(b);
        if (a && b && !isNaN(da) && !isNaN(db) && /\d{4}/.test(a) && /\d{4}/.test(b)) {
            return da - db;
        }
        return a.localeCompare(b, undefined, { sensitivity: 'base', numeric: true });
    }

    function clearIndicators(headerRow) {
        Array.from(headerRow.cells).forEach(cell => {
            cell.removeAttribute('data-sort-dir');
            const arrow = cell.querySelector('.sort-arrow');
            if (arrow) arrow.remove();
        });
    }

    function sortTable(table, columnIndex, ascending) {
        const headerRow = getHeaderRow(table);
        if (!headerRow) return;
        const dataRows = Array.from(table.rows).slice(1);
        dataRows.sort((r1, r2) => {
            const cmp = compareValues(cellText(r1, columnIndex), cellText(r2, columnIndex));
            return ascending ? cmp : -cmp;
        });
        dataRows.forEach(row => row.parentElement.appendChild(row));
    }

    document.addEventListener('click', function (event) {
        const th = event.target.closest('th');
        if (!th) return;
        const table = th.closest('table');
        if (!table || table.getAttribute('data-sortable') !== 'true') return;
        const headerRow = getHeaderRow(table);
        if (!headerRow || th.parentElement !== headerRow) return;

        const columnIndex = Array.from(headerRow.cells).indexOf(th);
        if (columnIndex < 0) return;

        const ascending = th.getAttribute('data-sort-dir') !== 'asc';
        clearIndicators(headerRow);
        th.setAttribute('data-sort-dir', ascending ? 'asc' : 'desc');
        const arrow = document.createElement('span');
        arrow.className = 'sort-arrow';
        arrow.style.marginLeft = '0.35rem';
        arrow.style.fontSize = '0.75em';
        arrow.textContent = ascending ? '▲' : '▼';
        th.appendChild(arrow);

        sortTable(table, columnIndex, ascending);
    });
})();
