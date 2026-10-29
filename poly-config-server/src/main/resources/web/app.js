const { createApp } = Vue;

async function api(path, options) {
    let response;
    try {
        response = await fetch(path, options);
    } catch (e) {
        throw new Error('无法连接服务端');
    }
    let body;
    try {
        body = await response.json();
    } catch (e) {
        throw new Error(`服务端返回异常 (${response.status})`);
    }
    if (!body || body.code !== 0) {
        throw new Error(body && body.message ? body.message : `请求失败 (${response.status})`);
    }
    return body.data;
}

function normalizeType(type) {
    return ['YAML', 'JSON', 'PROPERTIES'].indexOf(type) >= 0 ? type : 'YAML';
}

const app = createApp({
    data() {
        return {
            activeView: 'configs',
            namespace: 'public',
            group: 'DEFAULT_GROUP',
            namespaces: [],
            groups: [],
            configs: [],
            search: '',
            loading: false,
            health: true,
            configPage: 1,
            configPageSize: 10,
            namespacePage: 1,
            namespacePageSize: 10,
            cluster: {
                clusterId: '',
                mode: '',
                currentNodeId: '',
                nodes: []
            },
            editorVisible: false,
            editor: {
                dataId: '',
                group: '',
                type: 'YAML',
                operator: 'console',
                content: '',
                originalContent: '',
                editing: false
            },
            diffVisible: false,
            diffRows: [],
            diffTruncated: false,
            diffDataId: '',
            diffGroup: '',
            diffType: '',
            validation: {
                checked: false,
                errors: [],
                warnings: []
            },
            validationTimer: null,
            historyVisible: false,
            historyDataId: '',
            historyGroup: '',
            history: [],
            namespaceDialogVisible: false,
            newNamespaceName: ''
        };
    },
    computed: {
        filteredConfigs() {
            const keyword = this.search.trim().toLowerCase();
            if (!keyword) {
                return this.configs;
            }
            return this.configs.filter(item =>
                (item.dataId || '').toLowerCase().indexOf(keyword) >= 0);
        },
        pagedConfigs() {
            const start = (this.configPage - 1) * this.configPageSize;
            return this.filteredConfigs.slice(start, start + this.configPageSize);
        },
        pagedNamespaces() {
            const start = (this.namespacePage - 1) * this.namespacePageSize;
            return this.namespaces.slice(start, start + this.namespacePageSize)
                .map(name => ({name: name}));
        },
        editorTitle() {
            return this.editor.editing ? '编辑配置' : '新建配置';
        },
        healthType() {
            return this.health ? 'success' : 'danger';
        },
        healthText() {
            return this.health ? 'UP' : 'DOWN';
        },
        diffAdds() {
            return this.diffRows.filter(row => row.type === 'add').length;
        },
        diffRemoves() {
            return this.diffRows.filter(row => row.type === 'remove').length;
        }
    },
    watch: {
        'editor.content'() {
            this.scheduleValidate();
        },
        'editor.type'() {
            this.scheduleValidate();
        }
    },
    methods: {
        async init() {
            try {
                await this.loadNamespaces();
                await this.loadGroups();
                await this.loadConfigs();
                await this.loadCluster();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async loadNamespaces() {
            const items = await api('/v1/cs/namespaces');
            this.namespaces = (items && items.length) ? items : ['public'];
            if (this.namespaces.indexOf(this.namespace) < 0) {
                this.namespace = this.namespaces[0];
            }
        },
        async loadGroups() {
            const params = new URLSearchParams({namespace: this.namespace});
            const items = await api('/v1/cs/groups?' + params.toString());
            this.groups = (items && items.length) ? items : ['DEFAULT_GROUP'];
            if (!this.group) {
                this.group = this.groups[0] || 'DEFAULT_GROUP';
            }
        },
        async loadConfigs() {
            this.loading = true;
            try {
                const params = new URLSearchParams({
                    namespace: this.namespace,
                    group: this.group
                });
                const items = await api('/v1/cs/configs/list?' + params.toString());
                this.configs = items || [];
                this.configPage = 1;
            } finally {
                this.loading = false;
            }
        },
        async loadCluster() {
            try {
                const cluster = await api('/v1/cs/cluster');
                this.cluster = cluster || {nodes: []};
                const nodes = this.cluster.nodes || [];
                const current = nodes.find(node => node.nodeId === this.cluster.currentNodeId)
                    || nodes[0];
                this.health = current ? !!current.healthy : false;
            } catch (e) {
                this.health = false;
                throw e;
            }
        },
        async switchView(view) {
            this.activeView = view;
            try {
                await this.loadCluster();
                if (view === 'configs') {
                    await this.loadConfigs();
                } else if (view === 'namespaces') {
                    await this.loadNamespaces();
                }
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async refreshCurrent() {
            try {
                await this.loadCluster();
                if (this.activeView === 'configs') {
                    await this.loadConfigs();
                } else if (this.activeView === 'namespaces') {
                    await this.loadNamespaces();
                }
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async onNamespaceChange() {
            this.group = 'DEFAULT_GROUP';
            this.configPage = 1;
            try {
                await this.loadGroups();
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async onGroupChange() {
            this.configPage = 1;
            try {
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        onSearchInput() {
            this.configPage = 1;
        },
        async resetFilters() {
            this.search = '';
            this.group = 'DEFAULT_GROUP';
            this.configPage = 1;
            try {
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        openCreate() {
            this.editor = {
                dataId: '',
                group: this.group || 'DEFAULT_GROUP',
                type: 'YAML',
                operator: 'console',
                content: '',
                originalContent: '',
                editing: false
            };
            this.resetValidation();
            this.editorVisible = true;
            this.scheduleValidate();
        },
        openEdit(row) {
            this.editor = {
                dataId: row.dataId,
                group: row.group || this.group,
                type: normalizeType(row.type),
                operator: row.operator || 'console',
                content: row.content || '',
                originalContent: row.content || '',
                editing: true
            };
            this.resetValidation();
            this.editorVisible = true;
            this.scheduleValidate();
        },
        resetValidation() {
            this.validation = {checked: false, errors: [], warnings: []};
        },
        scheduleValidate() {
            if (!this.editorVisible) {
                return;
            }
            if (this.validationTimer) {
                window.clearTimeout(this.validationTimer);
            }
            this.validationTimer = window.setTimeout(() => this.validateContent(), 300);
        },
        validateContent() {
            const type = this.editor.type || 'YAML';
            const content = this.editor.content || '';
            const lines = splitLines(content);
            const errors = [];
            const warnings = [];
            const trimmedContent = content.trim();

            if (!trimmedContent) {
                if (type === 'JSON') {
                    errors.push('JSON 内容为空');
                } else {
                    warnings.push('配置内容为空');
                }
            }

            if (type === 'JSON') {
                let valid = false;
                if (trimmedContent) {
                    try {
                        JSON.parse(content);
                        valid = true;
                    } catch (e) {
                        errors.push('JSON 格式错误: ' + e.message);
                    }
                }
                if (!valid && (/\/\//.test(content) || /\/\*/.test(content))) {
                    warnings.push('JSON 不支持注释，请移除 // 或 /* */');
                }
            } else if (type === 'YAML') {
                if (trimmedContent && window.jsyaml) {
                    try {
                        window.jsyaml.load(content);
                    } catch (e) {
                        const mark = e.mark || {};
                        const line = mark.line != null ? mark.line + 1 : '?';
                        const column = mark.column != null ? mark.column + 1 : '?';
                        errors.push(`YAML 格式错误: 第 ${line} 行第 ${column} 列 ${e.reason || e.message}`);
                    }
                }
            } else if (type === 'PROPERTIES') {
                errors.push(...validateProperties(lines));
            }

            if (type === 'YAML' || type === 'PROPERTIES') {
                lines.forEach((line, index) => {
                    if (line.trim().startsWith('//')) {
                        warnings.push(`第 ${index + 1} 行使用了 // 注释，${type} 应使用 # 注释`);
                    }
                });
            }

            const punctuation = /[，。；：？！、（）【】｛｝“”‘’《》〈〉…—～]|　/g;
            let punctuationCount = 0;
            lines.forEach((line, index) => {
                const trimmed = line.trim();
                if (!trimmed) {
                    return;
                }
                if (type === 'PROPERTIES' && (trimmed.startsWith('#') || trimmed.startsWith('!'))) {
                    return;
                }
                if (type === 'YAML' && trimmed.startsWith('#')) {
                    return;
                }
                const scan = type === 'YAML' ? line.replace(/#.*$/, '') : line;
                const match = scan.match(punctuation);
                if (match) {
                    punctuationCount++;
                    if (punctuationCount <= 8) {
                        warnings.push(`第 ${index + 1} 行包含中文标点「${match[0]}」，请确认是否应使用英文符号`);
                    }
                }
            });
            if (punctuationCount > 8) {
                warnings.push(`还有 ${punctuationCount - 8} 行包含中文标点...`);
            }

            this.validation = {checked: true, errors: errors, warnings: warnings};
        },
        async openDiff() {
            const dataId = (this.editor.dataId || '').trim();
            if (!dataId) {
                this.$message.error('Data ID 不能为空');
                return;
            }
            this.validateContent();
            if (this.validation.errors.length) {
                this.$message.error('格式校验未通过，请先修正');
                return;
            }
            if (this.validation.warnings.length) {
                try {
                    await this.$confirm(
                        `检测到 ${this.validation.warnings.length} 条格式警告，是否继续发布？`,
                        '格式警告',
                        {
                            type: 'warning',
                            confirmButtonText: '继续发布',
                            cancelButtonText: '返回修改'
                        });
                } catch (e) {
                    return;
                }
            }
            const group = this.editor.group || this.group;
            let oldContent = this.editor.originalContent || '';
            try {
                const params = new URLSearchParams({
                    namespace: this.namespace,
                    group: group,
                    dataId: dataId
                });
                const current = await api('/v1/cs/configs?' + params.toString());
                oldContent = current && current.content != null ? current.content : '';
            } catch (e) {
                // 新配置或服务端不可用时，使用打开编辑器时的内容
            }
            const newContent = this.editor.content || '';
            if (this.editor.editing && oldContent === newContent) {
                this.$message.info('配置内容没有变化');
                return;
            }
            const result = buildDiff(oldContent, newContent);
            this.diffRows = result.rows;
            this.diffTruncated = result.truncated;
            this.diffDataId = dataId;
            this.diffGroup = group;
            this.diffType = this.editor.type;
            this.diffVisible = true;
        },
        async confirmPublish() {
            this.validateContent();
            if (this.validation.errors.length) {
                this.$message.error('格式校验未通过，请先修正');
                this.diffVisible = false;
                return;
            }
            this.diffVisible = false;
            await this.publishConfig();
        },
        async publishConfig() {
            const dataId = (this.editor.dataId || '').trim();
            if (!dataId) {
                this.$message.error('Data ID 不能为空');
                return;
            }
            this.validateContent();
            if (this.validation.errors.length) {
                this.$message.error('格式校验未通过，请先修正');
                return;
            }
            try {
                const result = await api('/v1/cs/configs', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        namespace: this.namespace,
                        group: this.editor.group || this.group,
                        dataId: dataId,
                        content: this.editor.content,
                        type: this.editor.type,
                        operator: this.editor.operator
                    })
                });
                this.$message.success(`发布成功，revision=${result.revision}`);
                this.editorVisible = false;
                await this.loadConfigs();
                await this.loadGroups();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async deleteConfig(row) {
            try {
                await this.$confirm(`确认删除 ${row.dataId}？`, '提示', {type: 'warning'});
            } catch (e) {
                return;
            }
            try {
                const params = new URLSearchParams({
                    namespace: this.namespace,
                    group: row.group || this.group,
                    dataId: row.dataId,
                    operator: 'console'
                });
                await api('/v1/cs/configs?' + params.toString(), {method: 'DELETE'});
                this.$message.success('删除成功');
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async deleteEditorConfig() {
            const dataId = (this.editor.dataId || '').trim();
            if (!dataId) {
                return;
            }
            try {
                await this.$confirm(`确认删除 ${dataId}？`, '提示', {type: 'warning'});
            } catch (e) {
                return;
            }
            try {
                const params = new URLSearchParams({
                    namespace: this.namespace,
                    group: this.editor.group || this.group,
                    dataId: dataId,
                    operator: this.editor.operator || 'console'
                });
                await api('/v1/cs/configs?' + params.toString(), {method: 'DELETE'});
                this.$message.success('删除成功');
                this.editorVisible = false;
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async openHistory(row) {
            this.historyDataId = row.dataId;
            this.historyGroup = row.group || this.group;
            await this.loadHistory();
            this.historyVisible = true;
        },
        async showHistoryFromEditor() {
            const dataId = (this.editor.dataId || '').trim();
            if (!dataId) {
                this.$message.error('Data ID 不能为空');
                return;
            }
            this.historyDataId = dataId;
            this.historyGroup = this.editor.group || this.group;
            this.editorVisible = false;
            await this.loadHistory();
            this.historyVisible = true;
        },
        async loadHistory() {
            try {
                const params = new URLSearchParams({
                    namespace: this.namespace,
                    group: this.historyGroup,
                    dataId: this.historyDataId,
                    limit: '50'
                });
                const items = await api('/v1/cs/history?' + params.toString());
                this.history = items || [];
            } catch (e) {
                this.$message.error(e.message);
                this.history = [];
            }
        },
        loadVersion(row) {
            this.editor = {
                dataId: this.historyDataId,
                group: this.historyGroup,
                type: normalizeType(row.type),
                operator: row.operator || 'console',
                content: row.content || '',
                originalContent: '',
                editing: true
            };
            this.resetValidation();
            this.historyVisible = false;
            this.editorVisible = true;
            this.scheduleValidate();
        },
        async rollback(row) {
            try {
                await this.$confirm(
                    `确认回滚 ${this.historyDataId} 到 revision ${row.revision}？`,
                    '提示', {type: 'warning'});
            } catch (e) {
                return;
            }
            try {
                const result = await api('/v1/cs/history/rollback', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        namespace: this.namespace,
                        group: this.historyGroup,
                        dataId: this.historyDataId,
                        revision: row.revision,
                        operator: 'console'
                    })
                });
                this.$message.success(`回滚成功，revision=${result.revision}`);
                await this.loadHistory();
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async enterNamespace(name) {
            this.namespace = name;
            this.group = 'DEFAULT_GROUP';
            this.configPage = 1;
            try {
                await this.loadGroups();
                await this.loadConfigs();
                this.activeView = 'configs';
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async createNamespace() {
            const name = (this.newNamespaceName || '').trim();
            if (!name) {
                this.$message.error('命名空间名称不能为空');
                return;
            }
            try {
                await api('/v1/cs/namespaces', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({namespace: name})
                });
                this.$message.success('命名空间已创建');
                this.namespaceDialogVisible = false;
                this.newNamespaceName = '';
                await this.loadNamespaces();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        formatTime(timestamp) {
            return timestamp ? new Date(timestamp).toLocaleString() : '-';
        },
        preview(content) {
            if (!content) {
                return '-';
            }
            const text = content.replace(/\s+/g, ' ').trim();
            return text.length > 50 ? text.slice(0, 50) + '…' : text;
        },
        operationType(operation) {
            if (operation === 'DELETE') {
                return 'danger';
            }
            if (operation === 'ROLLBACK') {
                return 'warning';
            }
            return 'success';
        }
    },
    mounted() {
        this.init();
    }
});

function splitLines(text) {
    const value = text == null ? '' : String(text);
    if (value === '') {
        return [];
    }
    return value.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
}

function validateProperties(lines) {
    const errors = [];
    let continued = false;
    lines.forEach((line, index) => {
        const text = line.trim();
        if (!text) {
            continued = false;
            return;
        }
        if (continued) {
            continued = line.endsWith('\\');
            return;
        }
        if (text.startsWith('#') || text.startsWith('!')) {
            continued = false;
            return;
        }
        if (text.startsWith('=') || text.startsWith(':')) {
            errors.push(`Properties 第 ${index + 1} 行 key 为空`);
        }
        continued = line.endsWith('\\');
    });
    if (continued) {
        errors.push('Properties 最后一行以续行符 \\ 结束');
    }
    return errors;
}

function buildDiff(oldText, newText) {
    const oldLines = splitLines(oldText);
    const newLines = splitLines(newText);
    const maxLines = 1500;
    if (oldLines.length > maxLines || newLines.length > maxLines) {
        const rows = [];
        let oldNo = 1;
        let newNo = 1;
        oldLines.slice(0, maxLines).forEach(text => {
            rows.push({type: 'remove', oldNo: oldNo++, newNo: null, text: text});
        });
        newLines.slice(0, maxLines).forEach(text => {
            rows.push({type: 'add', oldNo: null, newNo: newNo++, text: text});
        });
        return {rows: rows, truncated: true};
    }

    const oldLength = oldLines.length;
    const newLength = newLines.length;
    const dp = new Array(oldLength + 1);
    for (let i = 0; i <= oldLength; i++) {
        dp[i] = new Uint32Array(newLength + 1);
    }
    for (let i = oldLength - 1; i >= 0; i--) {
        for (let j = newLength - 1; j >= 0; j--) {
            dp[i][j] = oldLines[i] === newLines[j]
                ? dp[i + 1][j + 1] + 1
                : Math.max(dp[i + 1][j], dp[i][j + 1]);
        }
    }

    const rows = [];
    let i = 0;
    let j = 0;
    let oldNo = 1;
    let newNo = 1;
    while (i < oldLength && j < newLength) {
        if (oldLines[i] === newLines[j]) {
            rows.push({type: 'equal', oldNo: oldNo++, newNo: newNo++, text: oldLines[i]});
            i++;
            j++;
        } else if (dp[i + 1][j] >= dp[i][j + 1]) {
            rows.push({type: 'remove', oldNo: oldNo++, newNo: null, text: oldLines[i]});
            i++;
        } else {
            rows.push({type: 'add', oldNo: null, newNo: newNo++, text: newLines[j]});
            j++;
        }
    }
    while (i < oldLength) {
        rows.push({type: 'remove', oldNo: oldNo++, newNo: null, text: oldLines[i++]});
    }
    while (j < newLength) {
        rows.push({type: 'add', oldNo: null, newNo: newNo++, text: newLines[j++]});
    }
    return {rows: rows, truncated: false};
}

app.use(ElementPlus);
app.mount('#app');
