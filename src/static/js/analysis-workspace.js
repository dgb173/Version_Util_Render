/* Shared analysis workspace. The original match DOM and its actions stay intact. */
function buildAnalysisWorkspace() {
    return `<div class="pre-context-full-dialog analysis-workspace">
      <aside class="aw-sidebar"><div class="aw-brand"><span class="aw-brand-mark">A</span> ANALYSIS<span class="aw-brand-sub">MATCH INTELLIGENCE</span></div>
        <span class="aw-nav-label">ESPACIO DE TRABAJO</span>
        <button class="aw-nav is-active" data-aw-view="history">▤ <span>Historial y fichas</span></button>
        <button class="aw-nav" data-aw-view="compare">⇄ <span>Comparar equipos</span></button>
        <button class="aw-nav" data-aw-view="patterns">⌕ <span>Patrones similares</span></button>
        <div class="aw-sidebar-note">Análisis en contexto<small>Historial, estadísticas y coincidencias en un mismo espacio.</small></div>
      </aside>
      <main class="aw-main"><header class="pre-context-full-header"><div class="pre-context-full-heading"><span class="pre-context-full-kicker">CENTRO DE ANÁLISIS</span><strong>Historial y fichas</strong><small class="aw-match-title"></small></div><div class="pre-context-full-tools"><button class="aw-export" type="button">Exportar CSV</button><button type="button" class="pre-context-full-close" aria-label="Cerrar análisis">Cerrar ×</button></div></header>
      <div class="aw-toolbar"><div><span class="aw-field-label">MUESTRA POR EQUIPO</span><div class="pre-context-full-range" role="group" aria-label="Partidos visibles por equipo"><button data-popup-limit="5">5 partidos</button><button data-popup-limit="10">10</button><button data-popup-limit="20">20</button><button data-popup-limit="all">Todos</button></div></div><label><span class="aw-field-label">BUSCAR RIVAL</span><input class="aw-search" type="search" placeholder="Nombre del equipo…"></label><label><span class="aw-field-label">RESULTADO</span><select class="aw-result"><option value="">Todos</option><option value="W">Victorias</option><option value="D">Empates</option><option value="L">Derrotas</option></select></label><label><span class="aw-field-label">LECTURA</span><select class="aw-density"><option value="comfortable">Cómoda</option><option value="compact">Compacta</option></select></label></div>
      <div class="aw-summary" aria-live="polite"></div><div class="aw-status" aria-live="polite"></div>
      <div class="pre-context-full-body"></div>
      <section class="aw-patterns" aria-label="Búsqueda de patrones"><details class="aw-reference" open><summary>Partido de referencia · fila completa</summary><div class="aw-reference-scroll"></div></details><div class="aw-pattern-intro"><div><span class="aw-field-label">EXPLORADOR HISTÓRICO</span><h2>Encuentra partidos con un contexto similar</h2><p>La búsqueda original de Pre-Cacheo utiliza el hándicap actual y los antecedentes de ambos equipos. Puedes ajustar los filtros en el explorador.</p></div><div class="aw-repeat-controls"><label><span class="aw-field-label">REPETIR CONDICIONES</span><select class="aw-repeat-mode"><option value="base">Patrón original</option><option value="local">Indirecta local + AH actual</option><option value="away">Indirecta visitante + AH actual</option><option value="both">Ambas indirectas + AH actual</option></select></label><button class="aw-launch-patterns">Buscar coincidencias</button></div></div><div class="aw-pattern-status" role="status"></div><iframe title="Explorador de patrones similares" hidden></iframe></section>
      </main></div>`;
}

function initializeAnalysisWorkspace(state) {
    const {overlay, moment} = state;
    const names = [...moment.querySelectorAll('.pre-context-team-name')].slice(0,2).map(el=>el.textContent.trim());
    overlay.querySelector('.aw-match-title').textContent = names.join('  /  ');
    state.query = ''; state.result = '';
    initializeAnalysisReference(state);
    const setView = view => {
        overlay.dataset.view = view;
        overlay.querySelectorAll('[data-aw-view]').forEach(button=>{const active=button.dataset.awView===view;button.classList.toggle('is-active',active);button.setAttribute('aria-pressed',String(active));});
        overlay.querySelector('.pre-context-full-heading strong').textContent = {history:'Historial y fichas',compare:'Comparar equipos',patterns:'Patrones similares'}[view];
        overlay.querySelector('.aw-patterns').classList.toggle('is-focused', view === 'patterns');
        if(view==='compare') {
            const scroller=overlay.querySelector('.pre-context-full-body');
            const comparison=moment.querySelector('.pre-context-custom-compare');
            if(comparison) scroller.scrollTop += comparison.getBoundingClientRect().top-scroller.getBoundingClientRect().top;
        }
    };
    overlay.querySelectorAll('[data-aw-view]').forEach(button=>button.onclick=()=>setView(button.dataset.awView));
    overlay.querySelector('.aw-search').oninput = event=>{state.query=event.target.value.trim().toLocaleLowerCase();refreshPreContextPopupLimit();};
    overlay.querySelector('.aw-result').onchange = event=>{state.result=event.target.value;refreshPreContextPopupLimit();};
    overlay.querySelector('.aw-density').onchange = event=>{overlay.dataset.density=event.target.value;};
    overlay.querySelector('.aw-export').onclick = () => {
        const rows=[['Equipo','Fecha','Liga','Rival','Resultado','AH']];
        moment.querySelectorAll('.pre-context-team').forEach(team=>team.querySelectorAll('tbody tr.pre-context-row').forEach(row=>{
            if(row.hidden || row.style.display==='none' || row.dataset.popupLimitHidden) return;
            rows.push([team.querySelector('.pre-context-team-name')?.textContent.trim(), ...['.ctx-date','.ctx-league','.ctx-rival','.ctx-score','.ctx-ah'].map(selector=>{const cell=row.querySelector(selector);return cell?.getAttribute('title')||cell?.textContent.trim()||'';})]);
        }));
        const csv='\uFEFF'+rows.map(row=>row.map(value=>'"'+String(value??'').replaceAll('"','""')+'"').join(';')).join('\r\n');
        const url=URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'}));const a=document.createElement('a');a.href=url;a.download='analisis-partidos.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
    };
    overlay.querySelector('.aw-launch-patterns').onclick = async event => {
        setView('patterns');
        const status=overlay.querySelector('.aw-pattern-status');
        const section=moment.querySelector('.pre-context-custom-compare');
        const id=section?.dataset.parentMatchId;
        if(typeof APP_PRECACHEO_ONLY!=='undefined' && APP_PRECACHEO_ONLY){status.textContent='El explorador está desactivado en la configuración de esta aplicación.';return;}
        let url;
        if(typeof searchPatterns==='function') url=await searchPatterns(id,event.currentTarget,true);
        else {const params=new URLSearchParams({auto_search:'1',source_match_id:id||''});const ah=section?.dataset.currentAh;if(ah) params.set('handicap',typeof toExplorerBucketValue==='function'?toExplorerBucketValue(ah):ah);url='/explorador?'+params;}
        if(!url){status.textContent='No hay datos suficientes del partido para preparar la búsqueda.';return;}
        const embeddedUrl = new URL(url, window.location.href);
        embeddedUrl.searchParams.set('analysis_embed', '1');
        const repeatMode=overlay.querySelector('.aw-repeat-mode').value;
        if(!applyAnalysisIndirectPreset(embeddedUrl, state, repeatMode, status)) return;
        const frame=overlay.querySelector('.aw-patterns iframe');
        const patterns=overlay.querySelector('.aw-patterns');
        patterns.classList.add('has-results');
        status.textContent='Cargando el explorador…';
        frame.onload=()=>{status.textContent='Ajusta los filtros para ampliar o afinar las coincidencias.';};
        frame.src=embeddedUrl.pathname+embeddedUrl.search;frame.hidden=false;
        overlay.querySelector('.aw-launch-patterns').textContent='Repetir búsqueda';
    };
    overlay.addEventListener('keydown',event=>{
        if(event.key!=='Tab')return;
        const focusable=[...overlay.querySelectorAll('button,input,select,a[href],iframe,[tabindex="0"]')].filter(el=>!el.disabled&&el.getClientRects().length);
        const first=focusable[0],last=focusable.at(-1);
        if(event.shiftKey&&document.activeElement===first){event.preventDefault();last?.focus();}
        else if(!event.shiftKey&&document.activeElement===last){event.preventDefault();first?.focus();}
    });
    setView('history');
}

function initializeAnalysisReference(state) {
    const id=state.moment.querySelector('.pre-context-custom-compare')?.dataset.parentMatchId;
    const target=state.overlay.querySelector('.aw-reference-scroll');
    const source=[...document.querySelectorAll('tr[data-match-id],tr[data-match_id]')].find(row=>(row.dataset.matchId||row.dataset.match_id)===id && !row.closest('.pre-context-full-overlay'));
    state.referenceData=typeof precacheoData!=='undefined' ? precacheoData?.[id] : null;
    if(!source){target.textContent='La fila original no está disponible en esta vista.';return;}
    const table=document.createElement('table');table.className='aw-reference-table';
    const head=source.closest('table')?.querySelector('thead');
    if(head){const copy=head.cloneNode(true);copy.querySelectorAll('input,select,button,.multiselect-container').forEach(el=>el.remove());table.append(copy);}
    const body=document.createElement('tbody');const row=source.cloneNode(true);
    row.removeAttribute('data-match-id');row.removeAttribute('data-match_id');
    row.querySelectorAll('input[type=checkbox]').forEach(el=>{el.disabled=true;el.removeAttribute('onchange');});
    body.append(row);table.append(body);table.querySelectorAll('[id]').forEach(el=>el.removeAttribute('id'));
    // A reference is read-only: avoid duplicated row actions mutating the original list.
    table.querySelectorAll('*').forEach(el=>[...el.attributes].filter(attr=>attr.name.startsWith('on')).forEach(attr=>el.removeAttribute(attr.name)));
    table.querySelectorAll('button').forEach(el=>el.disabled=true);
    // Drop operational columns, preserving every analytical cell.
    const headers=[...table.querySelectorAll('thead tr:first-child th')];
    const removeColumns=headers.map((cell,index)=>({cell,index})).filter(({cell,index})=>/acciones|actions/i.test(cell.textContent) || (index===0 && source.cells[0]?.querySelector('input[type=checkbox]'))).map(({index})=>index).reverse();
    table.querySelectorAll('tr').forEach(tr=>removeColumns.forEach(index=>tr.cells[index]?.remove()));
    table.querySelectorAll('button,input,select,.liga-fav-btn,.liga-neutral-btn').forEach(el=>el.remove());
    target.append(table);
}

function applyAnalysisIndirectPreset(url, state, mode, status) {
    if(mode==='base'){
        state.overlay.querySelector('.aw-reference summary').textContent='Partido de referencia · fila completa';
        return true;
    }
    const pc=state.referenceData;
    const sides=mode==='both'?['local','away']:[mode];
    const currentAh=Number.parseFloat(String(pc?.main_match_odds?.ah_linea ?? pc?.handicap ?? '').replace(',','.'));
    const applied=[];
    for(const side of sides){
        const item=pc?.comparativas_indirectas?.[side==='local'?'left':'right'];
        const ah=item?.ah_line ?? item?.ah;
        const bucket=ah===null || ah===undefined || ah==='' ? null : (typeof toExplorerBucketValue==='function'?toExplorerBucketValue(ah):ah);
        if(!item || bucket===null || bucket===undefined || bucket===''){
            status.textContent=`No hay hándicap disponible en la indirecta ${side==='local'?'local':'visitante'}. Elige otra condición.`;return false;
        }
        const prefix=side==='local'?'ind_local':'ind_visitante';
        url.searchParams.set(prefix+'_ah',String(bucket));
        const loc=String(item.localia||'').toUpperCase();
        const normalizedLoc=['L','H','LOCAL','HOME'].includes(loc)?'H':(['A','V','VISITANTE','AWAY'].includes(loc)?'A':'');
        if(normalizedLoc)url.searchParams.set(prefix+'_loc',normalizedLoc);
        let cover=typeof getIndCoverStatus==='function' && Number.isFinite(currentAh) && item.score && normalizedLoc ? getIndCoverStatus(item.score, item.localia, side==='local', currentAh) : null;
        cover=cover||item.cover_status;
        cover=({'CUBIERTO':'COVER','NO CUBIERTO':'NO_COVER'})[cover]||cover;
        if(['COVER','NO_COVER','PUSH'].includes(cover))url.searchParams.set(prefix+'_res',cover);
        const coverLabel={COVER:'Cubierto',NO_COVER:'No cubierto',PUSH:'Push'}[cover];
        applied.push(`${side==='local'?'Local':'Visitante'}: AH ${bucket}${normalizedLoc?' · '+normalizedLoc:''}${coverLabel?' · '+coverLabel:''}`);
    }
    // An indirect search replaces the previous-match constraints, retaining current AH.
    ['prev_home_ah','prev_home_res','prev_away_ah','prev_away_res'].forEach(key=>url.searchParams.delete(key));
    state.overlay.querySelector('.aw-reference summary').textContent='Partido de referencia · '+applied.join(' / ');
    return true;
}

function refreshAnalysisSummary(state) {
    const summary=state.overlay.querySelector('.aw-summary');if(!summary)return;
    summary.replaceChildren();let total=0;
    state.moment.querySelectorAll('.pre-context-team').forEach(team=>{
        const rows=[...team.querySelectorAll('tbody tr.pre-context-row')].filter(row=>!row.hidden&&row.style.display!=='none'&&!row.dataset.popupLimitHidden);
        const card=document.createElement('div');card.className='aw-summary-card';
        const name=document.createElement('strong');name.textContent=team.querySelector('.pre-context-team-name')?.textContent.trim()||'Equipo';card.append(name);
        const stats=document.createElement('span');const count=result=>rows.filter(row=>row.dataset.contextResult===result).length;
        stats.textContent=`${rows.length} partidos visibles · ${count('W')} V · ${count('D')} E · ${count('L')} D`;card.append(stats);
        const form=document.createElement('div');form.className='aw-form';rows.slice(0,10).forEach(row=>{const badge=document.createElement('span');const result=row.dataset.contextResult;badge.dataset.result=result;badge.textContent={W:'V',D:'E',L:'D'}[result]||'—';form.append(badge);});card.append(form);summary.append(card);total+=rows.length;
    });
    state.overlay.querySelector('.aw-status').textContent=total?`${total} partidos visibles entre ambos equipos. Resumen calculado sobre la muestra mostrada. Selecciona las casillas para comparar rivales.`:'No hay partidos que coincidan con los filtros. Prueba otro rival o amplía la muestra.';
}
