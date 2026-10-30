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

function compareNames(a, b) {
    return String(a).localeCompare(String(b), 'zh-CN');
}

function filterNames(list, keyword) {
    const needle = (keyword || '').trim().toLowerCase();
    if (!needle) {
        return list;
    }
    return list.filter(item => String(item).toLowerCase().indexOf(needle) >= 0);
}

const PAGE_SIZE = 10;

const app = createApp({
    data() {
        return {
            activeView: 'configs',
            namespace: 'public',
            group: 'DEFAULT_GROUP',
            namespaces: [],
            groups: [],
            namespaceFilter: '',
            groupFilter: '',
            configs: [],
            search: '',
            loading: false,
            refreshing: false,
            propagating: false,
            health: true,
            configPage: 1,
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
                editing: false,
                revision: null
            },
            checkExpanded: false,
            diffVisible: false,
            diffRows: [],
            diffTruncated: false,
            diffOverwrite: false,
            diffFromRevision: null,
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
        filteredNamespaces() {
            return filterNames(this.namespaces, this.namespaceFilter);
        },
        filteredGroups() {
            return filterNames(this.groups, this.groupFilter);
        },
        filteredConfigs() {
            const keyword = this.search.trim().toLowerCase();
            if (!keyword) {
                return this.configs;
            }
            return this.configs.filter(item =>
                (item.dataId || '').toLowerCase().indexOf(keyword) >= 0);
        },
        pagedConfigs() {
            const start = (this.configPage - 1) * PAGE_SIZE;
            return this.filteredConfigs.slice(start, start + PAGE_SIZE);
        },
        pageCount() {
            return Math.max(1, Math.ceil(this.filteredConfigs.length / PAGE_SIZE));
        },
        rangeStart() {
            return this.filteredConfigs.length ? (this.configPage - 1) * PAGE_SIZE + 1 : 0;
        },
        rangeEnd() {
            return Math.min(this.configPage * PAGE_SIZE, this.filteredConfigs.length);
        },
        healthyCount() {
            return (this.cluster.nodes || []).filter(node => node.healthy).length;
        },
        clusterTitle() {
            const leader = this.cluster.nodes
                ? (this.cluster.nodes.find(node => node.role === 'LEADER') || {}).nodeId
                : null;
            return (this.cluster.clusterId || 'cluster')
                + ' · ' + (this.cluster.mode || '—')
                + ' · leader ' + (leader || '—');
        },
        checkState() {
            if (!this.validation.checked) {
                return 'idle';
            }
            if (this.validation.errors.length) {
                return 'error';
            }
            return this.validation.warnings.length ? 'warn' : 'ok';
        },
        checkTitle() {
            if (!this.validation.checked) {
                return '等待输入内容，校验结果会显示在这里';
            }
            if (this.validation.errors.length) {
                const extra = this.validation.warnings.length
                    ? ` · ${this.validation.warnings.length} 条提示`
                    : '';
                return `格式校验未通过 · ${this.validation.errors.length} 个错误${extra}`;
            }
            if (this.validation.warnings.length) {
                return `有 ${this.validation.warnings.length} 条提示，可以继续发布`;
            }
            return '格式校验通过';
        },
        diffAdds() {
            return this.diffRows.filter(row => row.type === 'add').length;
        },
        diffRemoves() {
            return this.diffRows.filter(row => row.type === 'remove').length;
        },
        checkItems() {
            if (!this.validation.checked) {
                return [];
            }
            return this.validation.errors.length
                ? this.validation.errors
                : this.validation.warnings;
        },
        contentPlaceholder() {
            const type = this.editor.type || 'YAML';
            if (type === 'JSON') {
                return '{\n  "key": "value"\n}';
            }
            if (type === 'PROPERTIES') {
                return 'key=value\nother.key=other-value';
            }
            return 'server:\n  port: 8080';
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
                await this.loadCluster();
                await this.loadNamespaces();
                await this.loadGroups();
                this.revealActive('.rail-groups');
                this.revealActive('.rail-ns');
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },

        /* ---------- 读取 ---------- */

        async loadNamespaces() {
            const items = await api('/v1/cs/namespaces');
            this.namespaces = (items && items.length) ? items.slice().sort(compareNames) : ['public'];
            if (this.namespaces.indexOf(this.namespace) < 0) {
                this.namespace = this.namespaces[0];
            }
        },
        async loadGroups() {
            const params = new URLSearchParams({namespace: this.namespace});
            const items = await api('/v1/cs/groups?' + params.toString());
            this.groups = (items && items.length)
                ? items.slice().sort(compareNames)
                : ['DEFAULT_GROUP'];
        },
        async loadConfigs() {
            this.loading = true;
            try {
                const params = new URLSearchParams({
                    namespace: this.namespace,
                    group: this.group
                });
                const items = await api('/v1/cs/configs/list?' + params.toString());
                const list = items || [];
                list.sort((a, b) => (b.modifiedTime || 0) - (a.modifiedTime || 0));
                this.configs = list;
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

        /* ---------- 导航 ---------- */

        async switchView(view) {
            if (this.activeView === view) {
                return;
            }
            this.activeView = view;
            try {
                await this.loadCluster();
                if (view === 'configs') {
                    await this.loadConfigs();
                }
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async refreshCurrent() {
            if (this.refreshing) {
                return;
            }
            this.refreshing = true;
            try {
                await this.loadCluster();
                if (this.activeView === 'configs') {
                    await this.loadNamespaces();
                    await this.loadGroups();
                    await this.loadConfigs();
                }
            } catch (e) {
                this.$message.error(e.message);
            } finally {
                this.refreshing = false;
            }
        },
        async selectNamespace(name) {
            this.namespaceFilter = '';
            this.groupFilter = '';
            if (name === this.namespace) {
                return;
            }
            this.namespace = name;
            this.search = '';
            this.configPage = 1;
            try {
                await this.loadGroups();
                if (this.groups.indexOf(this.group) < 0) {
                    this.group = this.groups[0] || 'DEFAULT_GROUP';
                }
                this.revealActive('.rail-groups');
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async selectGroup(name) {
            this.groupFilter = '';
            if (name === this.group) {
                return;
            }
            this.group = name;
            this.search = '';
            this.configPage = 1;
            this.revealActive('.rail-groups');
            try {
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        revealActive(scope) {
            this.$nextTick(() => {
                const item = document.querySelector(scope + ' .rail-item.active');
                if (item) {
                    item.scrollIntoView({block: 'nearest'});
                }
            });
        },
        pickFirstNamespace() {
            if (this.filteredNamespaces.length) {
                this.selectNamespace(this.filteredNamespaces[0]);
            }
        },
        pickFirstGroup() {
            if (this.filteredGroups.length) {
                this.selectGroup(this.filteredGroups[0]);
            }
        },
        async onNamespaceChange() {
            this.search = '';
            this.configPage = 1;
            try {
                await this.loadGroups();
                if (this.groups.indexOf(this.group) < 0) {
                    this.group = this.groups[0] || 'DEFAULT_GROUP';
                }
                this.revealActive('.rail-groups');
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        async onGroupChange() {
            this.search = '';
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
        clearSearch() {
            this.search = '';
            this.configPage = 1;
        },

        /* ---------- 编辑器 ---------- */

        openCreate() {
            this.editor = {
                dataId: '',
                group: this.group || 'DEFAULT_GROUP',
                type: 'YAML',
                operator: 'console',
                content: '',
                originalContent: '',
                editing: false,
                revision: null
            };
            this.checkExpanded = false;
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
                editing: true,
                revision: row.revision != null ? row.revision : null
            };
            this.checkExpanded = false;
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

        /* ---------- 发布 ---------- */

        async openDiff() {
            const dataId = (this.editor.dataId || '').trim();
            if (!dataId) {
                this.$message.error('Data ID 不能为空');
                this.editorVisible = false;
                return;
            }
            this.editor.dataId = dataId;
            this.validateContent();
            if (this.validation.errors.length) {
                this.checkExpanded = true;
                this.$message.error('格式校验未通过，请先修正');
                return;
            }
            if (this.validation.warnings.length) {
                try {
                    await this.$confirm(
                        `检测到 ${this.validation.warnings.length} 条格式提示，是否继续发布？`,
                        '格式提示',
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
            let oldRevision = null;
            try {
                const params = new URLSearchParams({
                    namespace: this.namespace,
                    group: group,
                    dataId: dataId
                });
                const current = await api('/v1/cs/configs?' + params.toString());
                oldContent = current && current.content != null ? current.content : '';
                oldRevision = current && current.revision != null ? current.revision : null;
            } catch (e) {
                // 新配置或服务端不可用时，使用打开编辑器时的内容
            }

            const newContent = this.editor.content || '';
            if (this.editor.editing && oldContent === newContent) {
                this.$message.info('配置内容没有变化');
                return;
            }
            if (!this.editor.editing && oldRevision != null) {
                this.diffOverwrite = true;
            } else {
                this.diffOverwrite = false;
            }

            const result = buildDiff(oldContent, newContent);
            this.diffRows = result.rows;
            this.diffTruncated = result.truncated;
            this.diffDataId = dataId;
            this.diffGroup = group;
            this.diffType = this.editor.type;
            this.diffFromRevision = oldRevision;
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
                this.$message.success(`已发布 rev ${result.revision}，正在推送给订阅客户端`);
                this.editorVisible = false;
                this.signalPropagation();
                await this.loadConfigs();
                await this.loadGroups();
            } catch (e) {
                this.$message.error(e.message);
            }
        },
        signalPropagation() {
            this.propagating = true;
            window.setTimeout(() => {
                this.propagating = false;
            }, 1000);
        },

        /* ---------- 删除 ---------- */

        async deleteConfig(row) {
            try {
                await this.$confirm(`确认删除 ${row.dataId}？删除后客户端会收到变更。`, '删除配置',
                    {type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消'});
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
                this.$message.success('已删除');
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
                await this.$confirm(`确认删除 ${dataId}？删除后客户端会收到变更。`, '删除配置',
                    {type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消'});
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
                this.$message.success('已删除');
                this.editorVisible = false;
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },

        /* ---------- 历史与回滚 ---------- */

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
                editing: true,
                revision: row.revision != null ? row.revision : null
            };
            this.checkExpanded = false;
            this.resetValidation();
            this.historyVisible = false;
            this.editorVisible = true;
            this.scheduleValidate();
        },
        async rollback(row) {
            try {
                await this.$confirm(
                    `确认把 ${this.historyDataId} 回滚到 rev ${row.revision}？`,
                    '回滚配置',
                    {type: 'warning', confirmButtonText: '回滚', cancelButtonText: '取消'});
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
                this.$message.success(`已回滚到 rev ${row.revision}，当前 rev ${result.revision}`);
                this.historyVisible = false;
                this.signalPropagation();
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },

        /* ---------- 命名空间 ---------- */

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
                this.$message.success(`已创建命名空间 ${name}`);
                this.namespaceDialogVisible = false;
                this.newNamespaceName = '';
                this.namespace = name;
                this.search = '';
                this.configPage = 1;
                await this.loadNamespaces();
                this.namespace = name;
                await this.loadGroups();
                this.group = this.groups[0] || 'DEFAULT_GROUP';
                await this.loadConfigs();
            } catch (e) {
                this.$message.error(e.message);
            }
        },

        /* ---------- 展示 ---------- */

        formatTime(timestamp) {
            if (!timestamp) {
                return '—';
            }
            const date = new Date(timestamp);
            const pad = value => (value < 10 ? '0' + value : '' + value);
            return date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate())
                + ' ' + pad(date.getHours()) + ':' + pad(date.getMinutes());
        },
        relativeTime(timestamp) {
            if (!timestamp) {
                return '—';
            }
            const diff = Date.now() - timestamp;
            if (diff < 0) {
                return this.formatTime(timestamp);
            }
            const minute = 60000;
            const hour = 60 * minute;
            const day = 24 * hour;
            if (diff < minute) {
                return '刚刚';
            }
            if (diff < hour) {
                return Math.floor(diff / minute) + ' 分钟前';
            }
            if (diff < day) {
                return Math.floor(diff / hour) + ' 小时前';
            }
            if (diff < 7 * day) {
                return Math.floor(diff / day) + ' 天前';
            }
            return this.formatTime(timestamp);
        },
        preview(content) {
            if (!content) {
                return '—';
            }
            const text = content.replace(/\s+/g, ' ').trim();
            return text.length > 160 ? text.slice(0, 160) + '…' : text;
        },
        typeLabel(type) {
            return normalizeType(type);
        },
        typeClass(type) {
            return normalizeType(type).toLowerCase();
        },
        roleLetter(role) {
            const name = (role || '').toUpperCase();
            if (name === 'LEADER') {
                return 'L';
            }
            if (name === 'FOLLOWER') {
                return 'F';
            }
            if (name === 'CANDIDATE') {
                return 'C';
            }
            if (name === 'STANDALONE') {
                return 'S';
            }
            return '·';
        },
        nodeTitle(node) {
            return (node.nodeId || '')
                + ' · ' + (node.role || '—')
                + ' · ' + (node.host || '—') + ':' + (node.port || '—')
                + (node.leaderId ? ' · leader ' + node.leaderId : '');
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
