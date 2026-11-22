/* Analyse the actual context panel and its current filters, including unsaved explorer fixtures. */
(() => {
    'use strict';
    const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const val=x=>x==null?'N/D':String(x);
    const signed=x=>x==null?'N/D':`${x>0?'+':''}${x}`;
    const labels={'1':'Gana','0.5':'½ ganancia','0':'Devuelve','-0.5':'½ pérdida','-1':'Pierde'};
    const outcome=x=>x==null?'N/D':labels[String(x)];
    const count=c=>`${c.win} G · ${c.half_win} ½G · ${c.push} Dev · ${c.half_loss} ½P · ${c.loss} P / ${c.n}`;
    let dialog,token=0;
    const requests=new Map();
    async function post(url,body,signal){
        const response=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),signal:signal||AbortSignal.timeout(25000)});
        let data;try{data=await response.json();}catch(_){throw new Error('El servidor no tiene cargada la versión actual del análisis de ficha.');}
        if(!response.ok||data.success===false||data.status==='error')throw new Error(data.error||`Error HTTP ${response.status}`);
        return data;
    }
    function snapshot(button){
        const panel=button.closest('.pre-context-panel');
        const id=button.dataset.fichaId;
        const source=window._preMatchContextMap?.[id]||{};
        const ctx=source.pre_match_context?.current||source.current||source;
        const p={...source,match_id:id,home_name:button.dataset.home,away_name:button.dataset.away,
            date:button.dataset.date,handicap:button.dataset.ah,goal_line:button.dataset.ou,
            ficha_rows:{home:[],away:[]},ficha_scope:[],row_references:{}};
        const previous=source.pre_match_context?.previous||source.previous;
        if(previous&&!p.h2h_general&&!p.h2h_stadium){
            p.h2h_general={all_matches:[{...previous,home:previous.home_name,away:previous.away_name,ahLine:previous.ah_line}]};
        }
        // The panel is authoritative; discard unselected history from other scopes.
        Object.keys(p).filter(k=>k.startsWith('recent_')||k==='pre_match_context').forEach(k=>delete p[k]);
        const moment=panel.querySelector('.pre-context-moment');
        if(!moment)throw new Error('La ficha todavía no tiene antecedentes cargados.');
        const known=[...(ctx.home_matches||[]),...(ctx.away_matches||[]),...(ctx.home_matches_all||[]),...(ctx.away_matches_all||[])];
        const jobs=[];
        moment.querySelectorAll('.pre-context-team').forEach(team=>{
            const role=team.dataset.defaultRole;
            if(!['home','away'].includes(role))return;
            const rows=[...team.querySelectorAll('tr.pre-context-row')].filter(r=>!r.hidden&&r.style.display!=='none');
            rows.forEach(row=>{
                const b=row.querySelector('.pre-context-col3-btn')||row.querySelector('[data-row-home]');
                if(!b)return;
                const d=b.dataset;
                const old=known.find(x=>String(x.match_id||x.matchIndex)===String(d.matchId))||{};
                const stats=row.querySelector('.pre-context-stats-btn')?.dataset||{};
                const r={...old,match_id:d.matchId,date:d.rowDate,home_team:d.rowHome,away_team:d.rowAway,score:d.rowScore,
                    ahLine:d.rowAh,ouLine:d.rowOu||stats.rowOu,league_id_hist:row.dataset.contextLeague};
                p.ficha_rows[role].push(r);
                jobs.push({row:r,dataset:{...d},role});
            });
        });
        p.recent_home_matches=p.ficha_rows.home;p.recent_away_matches=p.ficha_rows.away;
        panel.querySelectorAll('.pre-context-actions input[type=checkbox]').forEach(c=>p.ficha_scope.push(`${c.closest('label')?.textContent.trim()||c.className}: ${c.checked?'sí':'no'}`));
        p.ficha_scope.push(`Filas: ${p.ficha_rows.home.length} local / ${p.ficha_rows.away.length} visitante`);
        return {packet:p,jobs};
    }
    const match=r=>`${esc(r.team)} vs ${esc(r.opponent)} · ${r.gf}:${r.ga} · ${esc(r.date)}`;
    function pattern(p){
        const frequent=[...p.ah_lines].sort((a,b)=>b.n-a.n)[0];
        const goals=[...p.ou_lines].sort((a,b)=>b.n-a.n)[0];
        return `<p><b>Lectura de esta muestra:</b> ${frequent?`el AH propio más repetido es ${signed(frequent.line)} (${frequent.n} casos; ${frequent.share}% de las líneas conocidas), ${frequent.frequent?'frecuente según el criterio indicado':'con evidencia insuficiente para llamarlo frecuente'}.`:'Faltan hándicaps originales.'} Con la exigencia actual: ${p.target_ah.win} coberturas completas, ${p.target_ah.half_win} medias coberturas y ${p.target_ah.push} devoluciones en ${p.target_ah.n} antecedentes. ${goals?`La línea de goles más repetida es ${goals.line} (${goals.n} casos).`:''}</p>`;
    }
    function lines(list,title){
        return `<h3>${title}</h3><div class="ua-scroll"><table class="ua-table"><thead><tr><th>Línea propia</th><th>Veces / peso</th><th>Frecuente / actual</th><th>Superó / liquidación</th><th>Victorias / empates</th></tr></thead><tbody>${list.map(x=>`<tr><td>${signed(x.line)}</td><td>${x.n} · ${x.share}%</td><td>${x.frequent?'Frecuente':'No frecuente'} ${x.same_current?'· ACTUAL':''}</td><td>${count(x.settlement)}</td><td>${x.wins}V / ${x.draws}E</td></tr>`).join('')||'<tr><td colspan="5">No hay líneas verificables.</td></tr>'}</tbody></table></div>`;
    }
    function render(a,progress){
        const b=a.base;
        return `<header class="ua-head"><div><div class="ua-eyebrow">Análisis de la ficha seleccionada</div><h2 id="fa-title">${esc(b.home)} — ${esc(b.away)}</h2><p>${b.favorite?`Favorito: ${esc(b.favorite)} ${signed(b.line)}`:'Sin favorito determinado'} · O/U ${val(b.ou)}</p></div><button class="ua-close" data-fa-close aria-label="Cerrar">×</button></header><div class="ua-body"><section class="ua-card"><h3>Muestra y filtros de esta ficha</h3><p>${a.scope.map(esc).join(' · ')}</p><p class="ua-muted">${esc(a.rules)}</p><p id="fa-progress" role="status">${esc(progress)}</p></section>
        <div class="ua-grid">${['home','away'].map(role=>a.profiles[role]).map(p=>`<section class="ua-card"><h3>${esc(p.team)} · ${p.n} antecedentes</h3><p><b>${p.wins} victorias · ${p.draws} empates · ${p.losses} derrotas</b></p><p>AH original: ${count(p.ah)}</p><p>Con el AH actual: ${count(p.target_ah)}</p><p>Over original: ${count(p.over)}</p><p>Over en la línea actual: ${count(p.target_over)}</p>${pattern(p)}${lines(p.ah_lines,'Frecuencia de hándicaps')}${lines(p.ou_lines,'Frecuencia de Over / Under')}<p><b>AH y goles en el mismo partido (n=${p.joint.n}):</b> cubre+over ${p.joint.cover_over} · cubre+under ${p.joint.cover_under} · falla+over ${p.joint.fail_over} · falla+under ${p.joint.fail_under} · con devolución ${p.joint.pushes}.</p></section>`).join('')}</div>
        <section class="ua-card"><h3>Rivales comunes · comparación directa de rendimientos</h3><p class="ua-muted">Un margen mayor indica mejor marcador. Un residual mayor indica mejor rendimiento frente a la línea de cada partido. Los cruces repetidos no son muestras independientes.</p><div class="ua-scroll"><table class="ua-table"><thead><tr><th>Rival común</th><th>Local actual</th><th>Visitante actual</th><th>Diferencia de margen</th><th>Diferencia ajustada AH</th></tr></thead><tbody>${a.common_rivals.map(r=>`<tr><td>${esc(r.opponent)}</td><td>${match(r.home)}</td><td>${match(r.away)}</td><td>${signed(r.margin_delta)}</td><td>${signed(r.residual_delta)} · ${esc(r.comparison)}</td></tr>`).join('')||'<tr><td colspan="5">No se encuentran rivales comunes en las filas seleccionadas.</td></tr>'}</tbody></table></div></section>
        <section class="ua-card"><h3>¿Mejoró respecto al enfrentamiento anterior de cada fila?</h3><div class="ua-scroll"><table class="ua-table"><thead><tr><th>Partido de la ficha</th><th>Enfrentamiento previo</th><th>AH propio antes → fila</th><th>Mejora de marcador / AH</th></tr></thead><tbody>${a.evolution.map(r=>`<tr><td>${match(r.row)}</td><td>${match(r.before)}</td><td>${signed(r.before.own_line)} → ${signed(r.row.own_line)}</td><td>${signed(r.margin_delta)} / ${signed(r.residual_delta)} · ${esc(r.verdict)}</td></tr>`).join('')||'<tr><td colspan="4">No hay enfrentamientos previos verificables cargados para estas filas.</td></tr>'}</tbody></table></div></section>
        <section class="ua-card"><h3>Col3 de los antecedentes · rival espejo y hándicap</h3><div class="ua-scroll"><table class="ua-table"><thead><tr><th>Antecedente</th><th>Col3 / orientación</th><th>AH espejo</th><th>Comparación ajustada</th></tr></thead><tbody>${a.row_col3.map(x=>{const c=x.comparison;return `<tr><td>${match(x.row)}</td><td>${c?.row?`${esc(c.row.home)} ${esc(c.row.score)} ${esc(c.row.away)} · ${esc(c.orientation)} · ${esc(x.scope)}`:esc(x.error||c?.reason||'Pendiente de cargar')}</td><td>${signed(c?.row?.own_line)}</td><td>${signed(c?.residual_delta)} · ${esc(c?.signal||'ND')}</td></tr>`;}).join('')}</tbody></table></div></section>
        <section class="ua-card"><h3>Último H2H del partido actual</h3>${b.h2h[0]?`<p>${match(b.h2h[0])}. AH entonces ${signed(b.h2h[0].own_line)} → actual ${signed(b.line)}. Liquidación original: ${outcome(b.h2h[0].actual_ah)}; con la actual: ${outcome(b.h2h[0].target_ah)}. Over actual: ${outcome(b.h2h[0].over_target)}.</p>`:'<p>No hay un H2H fechado y orientado verificable en esta ficha.</p>'}<p class="ua-muted">Las líneas y comparaciones describen esta muestra. Las medias ganancias y pérdidas se conservan y los datos ausentes se señalan.</p></section><button class="ua-button" data-fa-close>Cerrar</button></div>`;
    }
    async function open(button){
        let task;try{task=snapshot(button);}catch(e){alert(e.message);return;}
        const run=++token;
        if(!dialog){dialog=document.createElement('dialog');dialog.className='ua-dialog';dialog.setAttribute('aria-labelledby','fa-title');document.body.appendChild(dialog);dialog.addEventListener('click',e=>{if(e.target===dialog||e.target.closest('[data-fa-close]'))dialog.close();});dialog.addEventListener('close',()=>token++);}
        dialog.innerHTML='<div class="ua-body" role="status">Analizando las filas y filtros de esta ficha…</div>';
        if(!dialog.open)dialog.showModal();
        try{
            const {packet,jobs}=task;
            let data=await post('/api/quant/ficha-analysis',packet);
            if(run!==token)return;
            dialog.innerHTML=render(data.analysis,`Cargando Col3 y movimientos de ${jobs.length} antecedentes…`);
            let done=0;let cursor=0;
            const worker=async()=>{while(cursor<jobs.length&&run===token){
                const job=jobs[cursor++],d=job.dataset;
                const body={match_id:d.matchId,parent_match_id:d.parentMatchId,parent_home:d.parentHome,parent_away:d.parentAway,parent_current_ah:d.currentAh,
                    row_date:d.rowDate,row_role:d.rowRole,row_home:d.rowHome,row_away:d.rowAway,row_home_id:d.rowHomeId,row_away_id:d.rowAwayId,row_score:d.rowScore,row_ah:d.rowAh};
                const key=JSON.stringify(body);
                if(!requests.has(key))requests.set(key,Promise.allSettled([
                    post('/api/precacheo_h2h_col3',body),post('/api/precacheo_h2h_mov_historico',body),
                    post('/api/match_stats',{...body,row_ou:job.row.ouLine,league_id:job.row.league_id_hist})
                ]).then(results=>({col3:results[0].status==='fulfilled'?results[0].value:null,movement:results[1].status==='fulfilled'?results[1].value:null,
                    stats:results[2].status==='fulfilled'?results[2].value:null,
                    error:results.some(r=>r.status==='rejected')?'Cruce no disponible; puedes volver a abrir la ficha para reintentar.':null})));
                const ref=await requests.get(key);packet.row_references[job.row.match_id]=ref;
                if(ref.stats){
                    if(ref.stats.stats?.length)job.row.stats_rows=ref.stats.stats;
                    if(!job.row.ouLine||['-','N/A'].includes(String(job.row.ouLine)))job.row.ouLine=ref.stats.ou_line;
                    if(!job.row.ahLine||['-','N/A'].includes(String(job.row.ahLine)))job.row.ahLine=ref.stats.ah_line;
                }
                if(ref.error)requests.delete(key);
                done++;if(run===token)dialog.querySelector('#fa-progress').textContent=`Cruces comprobados: ${done}/${jobs.length}.`;
            }};
            await Promise.all(Array.from({length:Math.min(3,jobs.length)},worker));
            if(run!==token)return;
            data=await post('/api/quant/ficha-analysis',packet);
            if(run!==token)return;
            const scroll=dialog.scrollTop;
            dialog.innerHTML=render(data.analysis,`Análisis completado: ${data.analysis.profiles.home.n+data.analysis.profiles.away.n} antecedentes válidos de ${packet.ficha_rows.home.length+packet.ficha_rows.away.length} filas seleccionadas. Cruces no disponibles señalados individualmente.`);dialog.scrollTop=scroll;
        }catch(e){if(run===token)dialog.innerHTML=`<div class="ua-body"><p role="alert">${esc(e.message)}</p><button class="ua-button" data-fa-close>Cerrar</button></div>`;}
    }
    document.addEventListener('click',e=>{const b=e.target.closest('[data-ficha-analysis]');if(b){e.preventDefault();open(b);}});
    window.FichaAnalysis={snapshot,open,render};
})();
