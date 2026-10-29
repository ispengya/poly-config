const $ = (id) => document.getElementById(id);

function showMessage(text, isError) {
    const element = $('message');
    element.textContent = text || '';
    element.className = isError ? 'error' : '';
}

async function call(path, options) {
    const response = await fetch(path, options);
    let body;
    try {
        body = await response.json();
    } catch (e) {
        throw new Error('响应不是 JSON');
    }
    if (!body || body.code !== 0) {
        throw new Error(body && body.message ? body.message : '请求失败');
    }
    return body.data;
}

function keyQuery() {
    return new URLSearchParams({
        namespace: $('namespace').value.trim(),
        group: $('group').value.trim(),
        dataId: $('dataId').value.trim()
    });
}

function formatTime(timestamp) {
    return timestamp ? new Date(timestamp).toLocaleString() : '';
}

function cell(text, className) {
    const td = document.createElement('td');
    td.textContent = text == null ? '' : String(text);
    if (className) {
        td.className = className;
    }
    return td;
}

function actionButton(text, onClick) {
    const button = document.createElement('button');
    button.className = 'small secondary';
    button.textContent = text;
    button.addEventListener('click', onClick);
    return button;
}

async function loadCluster() {
    try {
        const data = await call('/v1/cs/cluster');
        const nodes = (data.nodes || [])
            .map(node => `${node.nodeId}:${node.role}${node.healthy ? '' : '(!)'}`)
            .join(', ');
        $('cluster').textContent = `${data.clusterId} · ${data.mode} · ${nodes}`;
    } catch (e) {
        $('cluster').textContent = '集群状态不可用: ' + e.message;
    }
}

async function loadList() {
    const tbody = $('configTable');
    tbody.innerHTML = '';
    try {
        const params = new URLSearchParams({
            namespace: $('namespace').value.trim(),
            group: $('group').value.trim()
        });
        const items = await call('/v1/cs/configs/list?' + params.toString());
        (items || []).forEach(item => {
            const row = document.createElement('tr');
            row.appendChild(cell(item.dataId, 'mono'));
            row.appendChild(cell(item.version));
            row.appendChild(cell(item.md5, 'mono'));
            row.appendChild(cell(formatTime(item.modifiedTime)));
            row.appendChild(cell(item.operator));
            const actions = document.createElement('td');
            actions.appendChild(actionButton('编辑', () => {
                $('dataId').value = item.dataId;
                $('type').value = item.type;
                $('content').value = item.content;
                showMessage('已载入 ' + item.dataId);
            }));
            row.appendChild(actions);
            tbody.appendChild(row);
        });
    } catch (e) {
        showMessage(e.message, true);
    }
}

async function publish() {
    const payload = {
        namespace: $('namespace').value.trim(),
        group: $('group').value.trim(),
        dataId: $('dataId').value.trim(),
        content: $('content').value,
        type: $('type').value,
        operator: $('operator').value.trim()
    };
    try {
        const result = await call('/v1/cs/configs', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        showMessage(`发布成功，revision=${result.revision}`);
        await loadList();
        await loadHistory();
    } catch (e) {
        showMessage(e.message, true);
    }
}

async function removeConfig() {
    if (!confirm('确认删除当前配置？')) {
        return;
    }
    const params = keyQuery();
    params.set('operator', $('operator').value.trim());
    try {
        const result = await call('/v1/cs/configs?' + params.toString(), {method: 'DELETE'});
        showMessage(`删除成功，revision=${result.revision}`);
        $('content').value = '';
        await loadList();
        await loadHistory();
    } catch (e) {
        showMessage(e.message, true);
    }
}

async function loadHistory() {
    const tbody = $('historyTable');
    tbody.innerHTML = '';
    const params = keyQuery();
    params.set('limit', '20');
    try {
        const items = await call('/v1/cs/history?' + params.toString());
        (items || []).forEach(item => {
            const row = document.createElement('tr');
            row.appendChild(cell(item.revision));
            row.appendChild(cell(item.operation));
            row.appendChild(cell(item.operator));
            row.appendChild(cell(formatTime(item.timestamp)));
            const actions = document.createElement('td');
            actions.appendChild(actionButton('回滚', () => rollback(item.revision)));
            row.appendChild(actions);
            tbody.appendChild(row);
        });
    } catch (e) {
        showMessage(e.message, true);
    }
}

async function loadConfig() {
    try {
        const item = await call('/v1/cs/configs?' + keyQuery().toString());
        $('type').value = item.type;
        $('content').value = item.content;
    } catch (e) {
        // 配置不存在时保持编辑框现状
    }
}

async function rollback(revision) {
    if (!confirm(`回滚到 revision ${revision}？`)) {
        return;
    }
    const payload = {
        namespace: $('namespace').value.trim(),
        group: $('group').value.trim(),
        dataId: $('dataId').value.trim(),
        revision: revision,
        operator: $('operator').value.trim()
    };
    try {
        const result = await call('/v1/cs/history/rollback', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        showMessage(`回滚成功，revision=${result.revision}`);
        await loadList();
        await loadHistory();
        await loadConfig();
    } catch (e) {
        showMessage(e.message, true);
    }
}

$('publish').addEventListener('click', publish);
$('delete').addEventListener('click', removeConfig);
$('loadHistory').addEventListener('click', loadHistory);
$('refreshList').addEventListener('click', loadList);

loadCluster();
loadList();
