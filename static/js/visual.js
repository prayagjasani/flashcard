(() => {
    const COLORS = { python: '#68a7ff', html: '#ff8f68', javascript: '#f5cf5b', css: '#a47cff', config: '#8a9aab' };
    const LABELS = { python: 'Python', html: 'HTML', javascript: 'JavaScript', css: 'CSS', config: 'Config' };
    const GLYPHS = { python: 'PY', html: '<>', javascript: 'JS', css: '{}', config: '··' };
    const svg = document.querySelector('#graph');
    const viewport = document.querySelector('#viewport');
    const edgeLayer = document.querySelector('#edges');
    const nodeLayer = document.querySelector('#nodes');
    let graph = { nodes: [], edges: [] };
    let visibleNodes = [], visibleEdges = [], selected = null;
    let transform = { x: 0, y: 0, k: 1 };
    let activeKinds = new Set(Object.keys(COLORS));
    let search = '';
    let dragNode = null, panStart = null, raf = null;
    let activity = [], selectedFlowEventId = null, activeFlowNodes = new Set(), activeFlowEdges = new Set(), flowTimer = null;
    let tourTimer = null, activeTour = null;
    const preview = document.querySelector('#appPreview');
    const GUIDED_FLOWS = [
        { id:'startup', icon:'↗', color:'#68a7ff', title:'How a page loads', subtitle:'URL → HTML → browser logic → data', nodes:['app.py','routers/screens.py','templates/index.html','static/js/index.js','routers/decks.py','services/storage.py'], steps:['FastAPI starts and registers the app','The screen router receives the URL','The browser receives the main HTML','Browser JavaScript starts the interface','An API router handles deck requests','Storage reads the persistent data'] },
        { id:'deck', icon:'▣', color:'#62e6bd', title:'How deck data appears', subtitle:'Click → API → service → storage', nodes:['templates/index.html','static/js/index.js','routers/decks.py','services/deck_service.py','services/cache.py','services/storage.py'], steps:['The learner interacts with the home screen','JavaScript sends a request','The decks router owns the endpoint','Shared deck logic prepares the result','Cached data avoids repeated work','R2 storage is the durable source'] },
        { id:'create', icon:'＋', color:'#ff8f68', title:'How a deck is created', subtitle:'Form → validation → audio → save', nodes:['templates/create.html','routers/decks.py','models.py','services/audio.py','services/storage.py'], steps:['The creation form collects input','The decks endpoint receives it','A model validates the request shape','Audio can be generated in background','The finished deck is saved to R2'] },
        { id:'ai', icon:'✦', color:'#a47cff', title:'How AI stories work', subtitle:'Vocabulary → AI → audio → storage', nodes:['templates/story.html','routers/stories.py','services/ai.py','services/audio.py','services/storage.py'], steps:['The story screen starts the request','The stories router coordinates the job','The AI service generates learning content','The audio service creates narration','Story data and audio are persisted'] },
    ];

    const el = (tag, attrs = {}) => {
        const item = document.createElementNS('http://www.w3.org/2000/svg', tag);
        Object.entries(attrs).forEach(([key, value]) => item.setAttribute(key, value));
        return item;
    };

    async function load() {
        document.querySelector('#statusText').textContent = 'Reading project…';
        try {
            const response = await fetch('/api/project-graph', { cache: 'no-store' });
            if (!response.ok) throw new Error(`Request failed (${response.status})`);
            graph = await response.json();
            seedPositions();
            buildFilters();
            buildGuide();
            applyFilters();
            document.querySelector('#statusText').textContent = `${graph.nodes.length} files · ${graph.edges.length} connections`;
        } catch (error) {
            document.querySelector('#statusText').textContent = error.message;
        }
    }

    function seedPositions() {
        const width = svg.clientWidth || 900, height = svg.clientHeight || 650;
        const folders = [...new Set(graph.nodes.map(n => n.folder))];
        const centers = new Map(folders.map((folder, i) => {
            const angle = (i / Math.max(folders.length, 1)) * Math.PI * 2 - Math.PI / 2;
            return [folder, { x: width / 2 + Math.cos(angle) * width * .27, y: height / 2 + Math.sin(angle) * height * .3 }];
        }));
        const counts = {};
        graph.nodes.forEach((node, index) => {
            const center = centers.get(node.folder);
            const slot = counts[node.folder] = (counts[node.folder] || 0) + 1;
            const angle = slot * 2.399 + index * .05;
            const radius = 20 + Math.sqrt(slot) * 30;
            node.x = center.x + Math.cos(angle) * radius;
            node.y = center.y + Math.sin(angle) * radius;
            node.vx = node.vy = 0;
        });
        // A short force-layout pass gives connected files proximity while retaining directory clusters.
        for (let tick = 0; tick < 110; tick++) simulate();
    }

    function simulate() {
        const byId = new Map(graph.nodes.map(n => [n.id, n]));
        graph.edges.forEach(edge => {
            const a = byId.get(edge.source), b = byId.get(edge.target);
            if (!a || !b) return;
            const dx = b.x - a.x || .1, dy = b.y - a.y || .1;
            const distance = Math.hypot(dx, dy), force = (distance - 115) * .0025;
            a.vx += dx / distance * force; a.vy += dy / distance * force;
            b.vx -= dx / distance * force; b.vy -= dy / distance * force;
        });
        for (let i = 0; i < graph.nodes.length; i++) for (let j = i + 1; j < graph.nodes.length; j++) {
            const a = graph.nodes[i], b = graph.nodes[j], dx = b.x - a.x || .1, dy = b.y - a.y || .1;
            const d2 = dx * dx + dy * dy;
            if (d2 < 7000) { const f = Math.min(.7, 90 / d2); a.vx -= dx * f; a.vy -= dy * f; b.vx += dx * f; b.vy += dy * f; }
        }
        graph.nodes.forEach(n => { n.vx *= .76; n.vy *= .76; n.x += n.vx; n.y += n.vy; });
    }

    function buildFilters() {
        const counts = graph.nodes.reduce((all, node) => ({ ...all, [node.kind]: (all[node.kind] || 0) + 1 }), {});
        document.querySelector('#filters').innerHTML = Object.keys(COLORS).filter(kind => counts[kind]).map(kind => `
            <button class="filter active" data-kind="${kind}" style="--type-color:${COLORS[kind]}">
                <i class="filter-dot"></i><span class="filter-name">${LABELS[kind]}</span><span class="filter-count">${counts[kind]}</span>
            </button>`).join('');
        document.querySelectorAll('.filter').forEach(button => button.addEventListener('click', () => {
            const kind = button.dataset.kind;
            activeKinds.has(kind) ? activeKinds.delete(kind) : activeKinds.add(kind);
            button.classList.toggle('active', activeKinds.has(kind));
            applyFilters();
        }));
    }

    function applyFilters() {
        visibleNodes = graph.nodes.filter(n => activeKinds.has(n.kind) && (!search || n.id.toLowerCase().includes(search)));
        const visible = new Set(visibleNodes.map(n => n.id));
        visibleEdges = graph.edges.filter(e => visible.has(e.source) && visible.has(e.target));
        document.querySelector('#visibleCount').textContent = `${visibleNodes.length}/${graph.nodes.length}`;
        document.querySelector('#emptyState').hidden = visibleNodes.length !== 0;
        if (selected && !visible.has(selected.id)) selectNode(null);
        render();
    }

    function render() {
        edgeLayer.replaceChildren(); nodeLayer.replaceChildren();
        const byId = new Map(graph.nodes.map(n => [n.id, n]));
        visibleEdges.forEach(edge => {
            const a = byId.get(edge.source), b = byId.get(edge.target);
            const path = el('path', { class: `graph-edge ${edge.relation}`, 'data-source': edge.source, 'data-target': edge.target });
            const dx = b.x - a.x, dy = b.y - a.y, bend = Math.min(35, Math.hypot(dx, dy) * .12);
            const mx = (a.x + b.x) / 2 - dy / Math.max(Math.hypot(dx, dy), 1) * bend;
            const my = (a.y + b.y) / 2 + dx / Math.max(Math.hypot(dx, dy), 1) * bend;
            path.setAttribute('d', `M${a.x},${a.y} Q${mx},${my} ${b.x},${b.y}`);
            edgeLayer.append(path);
        });
        visibleNodes.forEach(node => {
            const group = el('g', { class: 'node', transform: `translate(${node.x} ${node.y})`, 'data-id': node.id, style: `--node-color:${COLORS[node.kind]}` });
            const radius = 13 + Math.min(7, Math.sqrt(node.connections || 0) * 1.5);
            group.append(el('circle', { class: 'halo', r: radius + 4 }), el('circle', { r: radius }));
            const glyph = el('text', { class: 'glyph', y: 1 }); glyph.textContent = GLYPHS[node.kind]; group.append(glyph);
            const label = el('text', { class: 'label', y: radius + 17 }); label.textContent = node.name.length > 23 ? `${node.name.slice(0, 21)}…` : node.name; group.append(label);
            group.addEventListener('pointerdown', event => startNodeDrag(event, node));
            group.addEventListener('click', event => { event.stopPropagation(); selectNode(node); });
            group.addEventListener('dblclick', event => { event.stopPropagation(); focusNode(node); });
            nodeLayer.append(group);
        });
        updateSelection(); updateTransform(); drawMinimap();
    }

    function updatePositions() {
        const byId = new Map(graph.nodes.map(n => [n.id, n]));
        edgeLayer.querySelectorAll('path').forEach(path => {
            const a = byId.get(path.dataset.source), b = byId.get(path.dataset.target), dx = b.x-a.x, dy = b.y-a.y;
            const bend = Math.min(35, Math.hypot(dx,dy)*.12), mx=(a.x+b.x)/2-dy/Math.max(Math.hypot(dx,dy),1)*bend, my=(a.y+b.y)/2+dx/Math.max(Math.hypot(dx,dy),1)*bend;
            path.setAttribute('d', `M${a.x},${a.y} Q${mx},${my} ${b.x},${b.y}`);
        });
        nodeLayer.querySelectorAll('.node').forEach(group => { const n=byId.get(group.dataset.id); group.setAttribute('transform', `translate(${n.x} ${n.y})`); });
        drawMinimap();
    }

    function selectNode(node) {
        selected = node;
        updateSelection();
        const empty = document.querySelector('#detailsEmpty'), content = document.querySelector('#detailsContent'), panel = document.querySelector('#details');
        empty.hidden = Boolean(node); content.hidden = !node; panel.classList.toggle('open', Boolean(node));
        if (!node) return;
        const related = graph.edges.filter(e => e.source === node.id || e.target === node.id);
        const incoming = related.filter(e => e.target === node.id).length;
        content.innerHTML = `<span class="type-badge" style="--badge-color:${COLORS[node.kind]}">${LABELS[node.kind]}</span>
            <h2 class="file-title">${escapeHtml(node.name)}</h2><div class="file-path">${escapeHtml(node.id)}</div><p class="file-purpose" style="--badge-color:${COLORS[node.kind]}">${escapeHtml(node.purpose || 'Project source file.')}</p>
            <div class="stats"><div class="stat"><strong>${related.length}</strong><span>Connections</span></div><div class="stat"><strong>${formatSize(node.size)}</strong><span>File size</span></div><div class="stat"><strong>${incoming}</strong><span>Incoming</span></div><div class="stat"><strong>${related.length-incoming}</strong><span>Outgoing</span></div></div>
            ${node.routes?.length ? `<h3 class="detail-section">Endpoints handled here</h3><div class="route-list">${node.routes.map(route => `<span class="route-chip"><b>${escapeHtml(route.method)}</b>${escapeHtml(route.path)}</span>`).join('')}</div>` : ''}
            ${node.symbols?.length ? `<h3 class="detail-section">Main code blocks</h3><div class="symbol-list">${node.symbols.map(symbol => `<span class="symbol-chip">${escapeHtml(symbol)}()</span>`).join('')}</div>` : ''}
            <h3 class="relation-title" style="margin-top:18px">Connected files</h3><div class="relation-list">${related.length ? related.map(edge => {
                const otherId = edge.source === node.id ? edge.target : edge.source, direction = edge.source === node.id ? 'outgoing' : 'incoming';
                const meaning = direction === 'outgoing' ? `This file ${edge.relation} ${otherId}` : `${otherId} ${edge.relation} this file`;
                return `<button class="relation-item" data-file="${escapeHtml(otherId)}">${escapeHtml(otherId)}<small>${escapeHtml(meaning)}</small></button>`;
            }).join('') : '<span class="file-path">No detected relationships</span>'}</div>`;
        content.querySelectorAll('[data-file]').forEach(button => button.addEventListener('click', () => selectNode(graph.nodes.find(n => n.id === button.dataset.file))));
    }

    function updateSelection() {
        const related = new Set();
        if (selected) graph.edges.forEach(e => { if (e.source === selected.id) related.add(e.target); if (e.target === selected.id) related.add(e.source); });
        nodeLayer.querySelectorAll('.node').forEach(g => {
            g.classList.toggle('selected', selected?.id === g.dataset.id);
            g.classList.toggle('dim', Boolean(selected) && selected.id !== g.dataset.id && !related.has(g.dataset.id));
            g.classList.toggle('flowing', activeFlowNodes.has(g.dataset.id));
        });
        edgeLayer.querySelectorAll('.graph-edge').forEach(p => {
            const isRelated = selected && (p.dataset.source === selected.id || p.dataset.target === selected.id);
            p.classList.toggle('related', Boolean(isRelated)); p.classList.toggle('dim', Boolean(selected) && !isRelated);
            p.classList.toggle('flowing', activeFlowEdges.has(`${p.dataset.source}|${p.dataset.target}`));
        });
    }

    function startNodeDrag(event, node) { event.stopPropagation(); dragNode = node; svg.setPointerCapture(event.pointerId); }
    function graphPoint(event) { const r=svg.getBoundingClientRect(); return { x:(event.clientX-r.left-transform.x)/transform.k, y:(event.clientY-r.top-transform.y)/transform.k }; }
    svg.addEventListener('pointerdown', event => { if (event.target === svg || event.target.closest('#viewport')) { panStart = { x:event.clientX-transform.x, y:event.clientY-transform.y }; svg.classList.add('panning'); } });
    svg.addEventListener('pointermove', event => {
        if (dragNode) { const point=graphPoint(event); dragNode.x=point.x; dragNode.y=point.y; updatePositions(); }
        else if (panStart) { transform.x=event.clientX-panStart.x; transform.y=event.clientY-panStart.y; updateTransform(); }
    });
    svg.addEventListener('pointerup', () => { dragNode=null; panStart=null; svg.classList.remove('panning'); });
    svg.addEventListener('click', event => { if (event.target === svg) selectNode(null); });
    svg.addEventListener('wheel', event => { event.preventDefault(); zoomAt(event.offsetX, event.offsetY, event.deltaY < 0 ? 1.12 : .89); }, { passive:false });

    function zoomAt(x, y, factor) { const old=transform.k; transform.k=Math.max(.25, Math.min(3, old*factor)); transform.x=x-(x-transform.x)*(transform.k/old); transform.y=y-(y-transform.y)*(transform.k/old); updateTransform(); }
    function updateTransform() { viewport.setAttribute('transform', `translate(${transform.x} ${transform.y}) scale(${transform.k})`); }
    function focusNode(node) { transform.k=1.45; transform.x=svg.clientWidth/2-node.x*transform.k; transform.y=svg.clientHeight/2-node.y*transform.k; selectNode(node); updateTransform(); }
    function fitGraph() {
        if (!visibleNodes.length) return;
        const xs=visibleNodes.map(n=>n.x), ys=visibleNodes.map(n=>n.y), minX=Math.min(...xs)-60, maxX=Math.max(...xs)+60, minY=Math.min(...ys)-60, maxY=Math.max(...ys)+60;
        transform.k=Math.max(.25, Math.min(1.5, Math.min(svg.clientWidth/(maxX-minX), svg.clientHeight/(maxY-minY))*.9));
        transform.x=(svg.clientWidth-(minX+maxX)*transform.k)/2; transform.y=(svg.clientHeight-(minY+maxY)*transform.k)/2; updateTransform();
    }

    function drawMinimap() {
        const mini=document.querySelector('#minimapGraph'); mini.replaceChildren(); if (!visibleNodes.length) return;
        const xs=visibleNodes.map(n=>n.x), ys=visibleNodes.map(n=>n.y), minX=Math.min(...xs)-30, maxX=Math.max(...xs)+30, minY=Math.min(...ys)-30, maxY=Math.max(...ys)+30;
        mini.setAttribute('viewBox', `${minX} ${minY} ${maxX-minX} ${maxY-minY}`);
        visibleEdges.forEach(e=>{const a=graph.nodes.find(n=>n.id===e.source),b=graph.nodes.find(n=>n.id===e.target);mini.append(el('line',{x1:a.x,y1:a.y,x2:b.x,y2:b.y,stroke:'#304258','stroke-width':2}));});
        visibleNodes.forEach(n=>mini.append(el('circle',{cx:n.x,cy:n.y,r:4,fill:COLORS[n.kind]})));
    }
    const escapeHtml = value => value.replace(/[&<>'"]/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char]));
    const formatSize = bytes => bytes < 1024 ? `${bytes} B` : `${(bytes/1024).toFixed(bytes > 10240 ? 0 : 1)} KB`;

    function routeMatches(pattern, path) {
        const expression = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\\\{[^}]+\\\}/g, '[^/]+');
        return new RegExp(`^${expression}$`).test(path);
    }

    function routeFor(path, method = 'GET') {
        const candidates = (graph.routes || []).filter(route => route.method === method && routeMatches(route.path, path));
        return candidates[0] || (graph.routes || []).find(route => routeMatches(route.path, path));
    }

    function previewContext() {
        let path = '/';
        try { path = preview.contentWindow.location.pathname; } catch (_) {}
        const pageRoute = routeFor(path, 'GET');
        const template = pageRoute?.template;
        const sources = new Set(template ? [template] : []);
        if (template) graph.edges.forEach(edge => {
            if (edge.source === template && edge.relation === 'loads' && edge.target.endsWith('.js')) sources.add(edge.target);
        });
        return { path, template, sources };
    }

    function highlightRequest(path, method) {
        const route = routeFor(path, method);
        const context = previewContext();
        const nodes = new Set(context.sources);
        const edges = new Set();
        if (route) {
            nodes.add(route.owner);
            graph.edges.forEach(edge => {
                if (edge.target === route.owner && edge.relation === 'calls' && context.sources.has(edge.source)) {
                    nodes.add(edge.source); edges.add(`${edge.source}|${edge.target}`);
                }
                if (edge.source === route.owner && edge.relation === 'imports') {
                    nodes.add(edge.target); edges.add(`${edge.source}|${edge.target}`);
                }
            });
        }
        if (context.template && route?.owner) {
            const direct = graph.edges.find(edge => edge.source === context.template && edge.target === route.owner);
            if (direct) edges.add(`${direct.source}|${direct.target}`);
        }
        activeFlowNodes = nodes; activeFlowEdges = edges; updateSelection();
        clearTimeout(flowTimer);
        flowTimer = setTimeout(() => { activeFlowNodes.clear(); activeFlowEdges.clear(); updateSelection(); }, 3200);
        if (route) {
            const ownerNode = graph.nodes.find(node => node.id === route.owner);
            if (ownerNode) selectNode(ownerNode);
        }
        return route;
    }

    function dataShape(value) {
        if (value == null || value === '') return '';
        try {
            const parsed = typeof value === 'string' ? JSON.parse(value) : value;
            if (Array.isArray(parsed)) return `array · ${parsed.length} items`;
            if (typeof parsed === 'object') return `object · ${Object.keys(parsed).slice(0, 7).join(', ')}`;
        } catch (_) {}
        return typeof value === 'string' ? `text · ${value.length} chars` : typeof value;
    }

    function addActivity(type, title, detail, id = null, meta = {}) {
        const item = { id: id || `${Date.now()}-${Math.random()}`, type, title, detail, meta, time: new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit', second:'2-digit'}) };
        const existing = activity.findIndex(entry => entry.id === item.id);
        if (existing >= 0) activity[existing] = { ...activity[existing], ...item, meta: { ...activity[existing].meta, ...meta } };
        else { activity.unshift(item); selectedFlowEventId = item.id; }
        activity = activity.slice(0, 40);
        renderActivity();
        return item.id;
    }

    function renderActivity() {
        const list = document.querySelector('#activityList');
        document.querySelector('#activityCount').textContent = `${activity.length} event${activity.length === 1 ? '' : 's'}`;
        if (!activity.length) {
            list.innerHTML = '<div class="activity-empty"><span>⌁</span><p>Click or perform an action in the preview to trace it here.</p></div>';
            return;
        }
        list.innerHTML = activity.map(item => `<button class="activity-item ${item.type}" data-event="${item.id}" style="display:block;width:100%;border-width:0 0 1px;background:transparent;text-align:left;cursor:pointer"><div class="activity-main"><strong>${item.type === 'click' ? 'UI' : item.type === 'error' ? 'ERR' : 'API'}</strong><span>${escapeHtml(item.title)}</span></div><div class="activity-sub">${escapeHtml(item.detail || '')}</div><time class="activity-time">${item.time}</time></button>`).join('');
        list.querySelectorAll('[data-event]').forEach(button => button.addEventListener('click', () => {
            selectedFlowEventId = button.dataset.event;
            setFlowView('diagram');
            renderFlowDiagram();
        }));
        renderFlowDiagram();
    }

    function flowStage(stage) {
        const tag = stage.file ? 'button' : 'div';
        const fileAttr = stage.file ? ` data-flow-file="${escapeHtml(stage.file)}"` : '';
        const services = stage.services?.length ? `<span class="flow-services">${stage.services.map(name => `<i>${escapeHtml(name)}</i>`).join('')}</span>` : '';
        return `<${tag} class="flow-stage ${stage.className || ''}"${fileAttr} style="--stage-color:${stage.color || '#68a7ff'}"><span class="flow-stage-icon">${stage.icon}</span><span class="flow-stage-copy"><small>${escapeHtml(stage.label)}</small><strong>${escapeHtml(stage.title)}</strong>${stage.detail ? `<span>${escapeHtml(stage.detail)}</span>` : ''}${services}</span></${tag}>`;
    }

    function renderFlowDiagram() {
        const container = document.querySelector('#flowDiagram');
        const item = activity.find(entry => entry.id === selectedFlowEventId) || activity[0];
        if (!item) {
            container.innerHTML = '<div class="activity-empty"><span>⇣</span><p>Use the app preview to generate a data-flow diagram.</p></div>';
            return;
        }
        const meta = item.meta || {};
        const stages = [];
        if (meta.kind === 'page') {
            stages.push(
                { icon:'URL', label:'Browser navigation', title:meta.path, detail:'The browser asks FastAPI for this page', color:'#68a7ff' },
                { icon:'PY', label:'Screen route', title:meta.owner || 'FastAPI router', detail:meta.handler || 'Matches the URL', file:meta.owner, color:'#68a7ff' },
                { icon:'<>', label:'HTML response', title:meta.template || 'HTML document', detail:'FastAPI sends this screen back', file:meta.template, color:'#ff8f68' },
                { icon:'UI', label:'Rendered result', title:'Visible application screen', detail:'The browser builds the interface', color:'#62e6bd', className:'response' },
            );
        } else if (meta.kind === 'click') {
            stages.push(
                { icon:'YOU', label:'User action', title:meta.label || item.title, detail:meta.target || 'A control was clicked', color:'#68a7ff' },
                { icon:'<>', label:'Current screen', title:meta.template || 'Browser page', detail:'Receives the click event', file:meta.template, color:'#ff8f68' },
                { icon:'JS', label:'Browser behavior', title:'Event handler runs', detail:'It may update the UI, navigate, or call an API', file:meta.source, color:'#f5cf5b' },
            );
        } else {
            const route = meta.route || routeFor(meta.path || '', meta.method || 'GET');
            const owner = meta.owner || route?.owner;
            const sources = meta.sources || [];
            const source = sources.find(path => path.endsWith('.js')) || sources[0];
            const dependencies = owner ? graph.edges.filter(edge => edge.source === owner && edge.relation === 'imports' && (edge.target.startsWith('services/') || edge.target === 'models.py' || edge.target === 'utils.py')).map(edge => edge.target).slice(0, 4) : [];
            if (source) stages.push({ icon:source.endsWith('.js')?'JS':'<>', label:'Request starts here', title:source, detail:meta.body ? `Sends ${meta.body}` : 'Builds the API request', file:source, color:source.endsWith('.js')?'#f5cf5b':'#ff8f68' });
            stages.push({ icon:'API', label:'HTTP request', title:`${meta.method || 'GET'} ${meta.path || item.title}`, detail:'FastAPI matches the URL and method', color:'#62e6bd' });
            if (owner) stages.push({ icon:'PY', label:'Route handler', title:owner, detail:meta.handler || route?.handler || 'Processes the request', file:owner, color:'#68a7ff' });
            if (dependencies.length) stages.push({ icon:'⚙', label:'Backend dependencies', title:'Services and shared code', detail:'Possible dependencies available to this router', services:dependencies, color:'#a47cff' });
            stages.push({ icon:meta.status && meta.status >= 400 ? '!' : '✓', label:'Response returns', title:meta.status ? `HTTP ${meta.status}` : 'Response data', detail:meta.responseShape || item.detail, color:meta.status && meta.status >= 400 ? '#ff6b6b' : '#62e6bd', className:meta.status && meta.status >= 400 ? 'error' : 'response' });
        }
        container.innerHTML = `<div class="flow-diagram-head"><small>Selected operation</small><strong>${escapeHtml(item.title)}</strong></div>${stages.map((stage, index) => `${index ? '<div class="flow-connector"></div>' : ''}${flowStage(stage)}`).join('')}`;
        container.querySelectorAll('[data-flow-file]').forEach(button => button.addEventListener('click', () => {
            const node = graph.nodes.find(candidate => candidate.id === button.dataset.flowFile);
            if (node) { selectNode(node); focusNode(node); }
        }));
    }

    function setFlowView(view) {
        const events = view === 'events';
        document.querySelector('.activity-panel').classList.toggle('event-mode', events);
        document.querySelector('#eventView').classList.toggle('active', events);
        document.querySelector('#diagramView').classList.toggle('active', !events);
    }

    function buildGuide() {
        const overview = graph.overview || {};
        document.querySelector('#guideSummary').textContent = overview.summary || 'Explore the files and follow their connections to understand the application.';
        document.querySelector('#layerFlow').innerHTML = (overview.layers || []).map(layer => `<div class="layer-item"><strong>${escapeHtml(layer.name)}</strong><span>${escapeHtml(layer.description)}</span></div>`).join('');
        document.querySelector('#flowCards').innerHTML = GUIDED_FLOWS.map(flow => `<button class="flow-card" data-flow="${flow.id}" style="--flow-color:${flow.color}"><span class="flow-icon">${flow.icon}</span><span class="flow-copy"><strong>${flow.title}</strong><small>${flow.subtitle}</small></span><span class="flow-arrow">›</span></button>`).join('');
        document.querySelectorAll('[data-flow]').forEach(button => button.addEventListener('click', () => playGuidedFlow(GUIDED_FLOWS.find(flow => flow.id === button.dataset.flow))));
    }

    function playGuidedFlow(flow) {
        if (!flow) return;
        stopGuidedFlow();
        activeTour = { flow, index: 0, playing: false };
        document.querySelector('#guidePanel').hidden = true;
        document.querySelector('#understandButton').classList.remove('active');
        document.querySelector('#tourStatus').hidden = false;
        showTourStep();
    }

    function showTourStep() {
        if (!activeTour) return;
        const { flow, index } = activeTour;
        const id = flow.nodes[index], node = graph.nodes.find(item => item.id === id);
        activeFlowNodes = new Set(flow.nodes.slice(0, index + 1));
        activeFlowEdges = new Set();
        for (let step = 1; step <= index; step++) {
            const a = flow.nodes[step - 1], b = flow.nodes[step];
            const edge = graph.edges.find(item => (item.source === a && item.target === b) || (item.source === b && item.target === a));
            if (edge) activeFlowEdges.add(`${edge.source}|${edge.target}`);
        }
        document.querySelector('#tourCounter').textContent = `${flow.title} · Step ${index + 1} of ${flow.nodes.length}`;
        document.querySelector('#tourStep').textContent = `${flow.steps[index]} — ${id}`;
        document.querySelector('#tourProgress').style.width = `${((index + 1) / flow.nodes.length) * 100}%`;
        document.querySelector('#tourBack').disabled = index === 0;
        document.querySelector('#tourNext').disabled = index === flow.nodes.length - 1;
        if (node) selectNode(node); else updateSelection();
    }

    function changeTourStep(direction) {
        if (!activeTour) return;
        const next = Math.max(0, Math.min(activeTour.flow.nodes.length - 1, activeTour.index + direction));
        if (next === activeTour.index) {
            if (activeTour.playing && next === activeTour.flow.nodes.length - 1) setTourPlaying(false);
            return;
        }
        activeTour.index = next;
        showTourStep();
        if (activeTour.playing) scheduleTourStep();
    }

    function scheduleTourStep() {
        clearTimeout(tourTimer);
        if (!activeTour?.playing) return;
        tourTimer = setTimeout(() => changeTourStep(1), 3500);
    }

    function setTourPlaying(playing) {
        if (!activeTour) return;
        activeTour.playing = playing;
        document.querySelector('#tourPlay').textContent = playing ? 'Ⅱ Pause' : '▶ Play';
        if (playing) scheduleTourStep(); else clearTimeout(tourTimer);
    }

    function stopGuidedFlow() {
        clearTimeout(tourTimer);
        activeTour = null;
        document.querySelector('#tourStatus').hidden = true;
        document.querySelector('#tourPlay').textContent = '▶ Play';
        activeFlowNodes.clear(); activeFlowEdges.clear(); updateSelection();
    }

    function toggleGuide(open) {
        document.querySelector('#guidePanel').hidden = !open;
        document.querySelector('#understandButton').classList.toggle('active', open);
    }

    function installPreviewTracing() {
        document.querySelector('#previewLoading').hidden = true;
        let win, doc;
        try { win = preview.contentWindow; doc = preview.contentDocument; } catch (_) { return; }
        if (!win || !doc || win.location.pathname === '/visual') return;
        // The first iframe load can finish before the graph metadata request.
        // Defer instrumentation until route ownership is available.
        if (!graph.routes?.length) return;
        document.querySelector('#previewPath').value = win.location.pathname + win.location.search;
        const pageRoute = routeFor(win.location.pathname, 'GET');
        addActivity('request', `PAGE ${win.location.pathname}`, pageRoute ? `${pageRoute.owner} → ${pageRoute.template || pageRoute.handler}` : 'Page loaded', null, {
            kind:'page', path:win.location.pathname, owner:pageRoute?.owner, handler:pageRoute?.handler, template:pageRoute?.template,
        });
        if (pageRoute) {
            activeFlowNodes = new Set([pageRoute.owner, pageRoute.template].filter(Boolean));
            activeFlowEdges = new Set(graph.edges
                .filter(edge => edge.source === pageRoute.owner && edge.target === pageRoute.template)
                .map(edge => `${edge.source}|${edge.target}`));
            updateSelection();
        }

        // Fetches made while the new page was starting happened before its load event.
        // Resource timing lets the parent recover those read operations for the trace.
        if (!win.__projectFlowSeenResources) win.__projectFlowSeenResources = new Set();
        win.performance.getEntriesByType('resource').forEach(entry => {
            const url = new URL(entry.name, win.location.href);
            const route = routeFor(url.pathname, 'GET');
            if (!route || win.__projectFlowSeenResources.has(entry.name)) return;
            win.__projectFlowSeenResources.add(entry.name);
            const context = previewContext();
            addActivity('request', `GET ${url.pathname}`, `${route.owner} · ${route.handler} · ${Math.round(entry.duration)}ms`, null, {
                kind:'api', method:'GET', path:url.pathname, owner:route.owner, handler:route.handler, route, sources:[...context.sources], responseShape:'Response observed during page load',
            });
            highlightRequest(url.pathname, 'GET');
        });

        doc.addEventListener('click', event => {
            const target = event.target.closest('button, a, [role="button"], input, select, .deck-tile');
            if (!target) return;
            const label = (target.getAttribute('aria-label') || target.textContent || target.value || target.id || target.tagName).trim().replace(/\s+/g, ' ').slice(0, 55);
            const context = previewContext();
            activeFlowNodes = new Set(context.sources); activeFlowEdges.clear(); updateSelection();
            const source = [...context.sources].find(path => path.endsWith('.js'));
            addActivity('click', label || target.tagName, `${context.template || context.path} · #${target.id || target.tagName.toLowerCase()}`, null, {
                kind:'click', label:label || target.tagName, target:`#${target.id || target.tagName.toLowerCase()}`, template:context.template, source,
            });
        }, true);

        if (!win.__projectFlowOriginalFetch) {
            win.__projectFlowOriginalFetch = win.fetch.bind(win);
            win.fetch = async (...args) => {
                const input = args[0], options = args[1] || {};
                const rawUrl = typeof input === 'string' ? input : input?.url || '';
                const url = new URL(rawUrl, win.location.href);
                const method = (options.method || input?.method || 'GET').toUpperCase();
                const eventId = `${Date.now()}-${Math.random()}`;
                const body = options.body ? dataShape(options.body) : '';
                const route = highlightRequest(url.pathname, method);
                const context = previewContext();
                const requestMeta = { kind:'api', method, path:url.pathname, owner:route?.owner, handler:route?.handler, route, sources:[...context.sources], body };
                addActivity('request', `${method} ${url.pathname}`, `${route ? `${route.owner} · ${route.handler}` : 'External or static request'}${body ? ` · sends ${body}` : ''}`, eventId, requestMeta);
                const started = performance.now();
                try {
                    const response = await win.__projectFlowOriginalFetch(...args);
                    const duration = Math.round(performance.now() - started);
                    const type = response.headers.get('content-type')?.split(';')[0] || 'response';
                    addActivity(response.ok ? 'request' : 'error', `${method} ${url.pathname}`, `${response.status} ${response.statusText} · ${type} · ${duration}ms`, eventId, { status:response.status, responseShape:`${type} · ${duration}ms` });
                    if (type.includes('json')) response.clone().json().then(data => {
                        const shape = dataShape(data);
                        addActivity(response.ok ? 'request' : 'error', `${method} ${url.pathname}`, `${response.status} · receives ${shape} · ${duration}ms`, eventId, { status:response.status, responseShape:`Receives ${shape} · ${duration}ms` });
                    }).catch(() => {});
                    return response;
                } catch (error) {
                    addActivity('error', `${method} ${url.pathname}`, error.message, eventId, { status:0, responseShape:error.message });
                    throw error;
                }
            };
        }
    }

    function navigatePreview(path) {
        let safePath = path.trim() || '/';
        if (!safePath.startsWith('/')) safePath = `/${safePath}`;
        if (safePath.startsWith('/visual')) safePath = '/';
        document.querySelector('#previewLoading').hidden = false;
        preview.src = safePath;
    }

    document.querySelector('#searchInput').addEventListener('input', event => { search=event.target.value.trim().toLowerCase(); applyFilters(); });
    document.addEventListener('keydown', event => {
        if (event.key==='/' && document.activeElement.tagName!=='INPUT') { event.preventDefault(); document.querySelector('#searchInput').focus(); }
        if (activeTour && document.activeElement.tagName!=='INPUT') {
            if (event.key==='ArrowLeft') { event.preventDefault(); setTourPlaying(false); changeTourStep(-1); }
            if (event.key==='ArrowRight') { event.preventDefault(); setTourPlaying(false); changeTourStep(1); }
            if (event.key===' ') { event.preventDefault(); setTourPlaying(!activeTour.playing); }
        }
        if (event.key==='Escape') activeTour ? stopGuidedFlow() : selectNode(null);
    });
    document.querySelector('#refreshButton').addEventListener('click', load);
    document.querySelector('#fitButton').addEventListener('click', fitGraph);
    document.querySelector('#detailsClose').addEventListener('click', () => selectNode(null));
    document.querySelector('#understandButton').addEventListener('click', () => toggleGuide(document.querySelector('#guidePanel').hidden));
    document.querySelector('#guideClose').addEventListener('click', () => toggleGuide(false));
    document.querySelector('#tourStop').addEventListener('click', stopGuidedFlow);
    document.querySelector('#tourBack').addEventListener('click', () => { setTourPlaying(false); changeTourStep(-1); });
    document.querySelector('#tourNext').addEventListener('click', () => { setTourPlaying(false); changeTourStep(1); });
    document.querySelector('#tourPlay').addEventListener('click', () => setTourPlaying(!activeTour?.playing));
    preview.addEventListener('load', installPreviewTracing);
    document.querySelector('#previewAddressForm').addEventListener('submit', event => { event.preventDefault(); navigatePreview(document.querySelector('#previewPath').value); });
    document.querySelector('#previewReload').addEventListener('click', () => { document.querySelector('#previewLoading').hidden=false; preview.contentWindow.location.reload(); });
    document.querySelector('#clearTrace').addEventListener('click', () => { activity=[]; selectedFlowEventId=null; activeFlowNodes.clear(); activeFlowEdges.clear(); renderActivity(); updateSelection(); });
    document.querySelector('#diagramView').addEventListener('click', () => setFlowView('diagram'));
    document.querySelector('#eventView').addEventListener('click', () => setFlowView('events'));
    const togglePreview = open => {
        document.querySelector('.center-stage').classList.toggle('preview-closed', !open);
        document.querySelector('#previewToggle').classList.toggle('active', open);
        setTimeout(fitGraph, 40);
    };
    document.querySelector('#previewToggle').addEventListener('click', () => togglePreview(document.querySelector('.center-stage').classList.contains('preview-closed')));
    document.querySelector('#runtimeClose').addEventListener('click', () => togglePreview(false));
    document.querySelectorAll('[data-zoom]').forEach(button => button.addEventListener('click', () => zoomAt(svg.clientWidth/2, svg.clientHeight/2, Number(button.dataset.zoom))));
    window.addEventListener('resize', () => { clearTimeout(raf); raf=setTimeout(fitGraph, 100); });
    togglePreview(true);
    load().then(() => { setTimeout(fitGraph, 30); toggleGuide(true); if (preview.contentDocument?.readyState === 'complete') installPreviewTracing(); });
})();
