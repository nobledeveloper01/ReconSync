(function(){let e=document.createElement(`link`).relList;if(e&&e.supports&&e.supports(`modulepreload`))return;for(let e of document.querySelectorAll(`link[rel="modulepreload"]`))n(e);new MutationObserver(e=>{for(let t of e)if(t.type===`childList`)for(let e of t.addedNodes)e.tagName===`LINK`&&e.rel===`modulepreload`&&n(e)}).observe(document,{childList:!0,subtree:!0});function t(e){let t={};return e.integrity&&(t.integrity=e.integrity),e.referrerPolicy&&(t.referrerPolicy=e.referrerPolicy),t.credentials=e.crossOrigin===`use-credentials`?`include`:e.crossOrigin===`anonymous`?`omit`:`same-origin`,t}function n(e){if(e.ep)return;e.ep=!0;let n=t(e);fetch(e.href,n)}})();var e=class extends Error{status;code;constructor(e,t,n){super(n),this.status=e,this.code=t}},t=`reconsync.key`;function n(){return sessionStorage.getItem(t)}function r(e){sessionStorage.setItem(t,e)}function i(){sessionStorage.removeItem(t)}async function a(t){let r=n();if(!r)throw new e(401,`unauthenticated`,`no API key`);let i=await fetch(t,{headers:{Authorization:`Bearer ${r}`}});if(!i.ok){let t=`error`,n=`${i.status} ${i.statusText}`;try{let e=await i.json();e?.error&&(t=e.error.code??t,n=e.error.message??n)}catch{}throw new e(i.status,t,n)}return await i.json()}var o={transactions:(e,t=100)=>a(`/v1/transactions?status=${encodeURIComponent(e)}&limit=${t}`),exposure:e=>a(`/v1/reports/exposure?scope=${e}`),compliance:()=>a(`/v1/reports/reversal-compliance`),scorecard:()=>a(`/v1/reports/providers`),windowFit:()=>a(`/v1/reports/window-fit`),audit:()=>a(`/v1/audit/verify`),licence:()=>a(`/v1/licence`),endpoints:()=>a(`/v1/webhooks`)};function s(e,t){return`${t} ${e.toLocaleString(`en-US`)}`}function c(e){return e<60?`${e.toFixed(1)}s`:e<3600?`${(e/60).toFixed(1)}m`:e<86400?`${(e/3600).toFixed(1)}h`:`${(e/86400).toFixed(1)}d`}function l(e){if(!e)return`—`;let t=new Date(e);return Number.isNaN(t.getTime())?e:t.toISOString().replace(`T`,` `).slice(0,19)+`Z`}function u(e){return e==null?`—`:`${(e*100).toFixed(1)}%`}function d(e){return String(e??``).replaceAll(`&`,`&amp;`).replaceAll(`<`,`&lt;`).replaceAll(`>`,`&gt;`).replaceAll(`"`,`&quot;`).replaceAll(`'`,`&#39;`)}function f(e,t){return t.length===0?`<p class="empty">Nothing to show.</p>`:`<table>
    <thead><tr>${e.map(e=>`<th>${d(e)}</th>`).join(``)}</tr></thead>
    <tbody>${t.map(e=>`<tr>${e.map(e=>`<td>${e}</td>`).join(``)}</tr>`).join(``)}</tbody>
  </table>`}function p(e,t,n=``){return`<div class="card">
    <div class="card-title">${d(e)}</div>
    <div class="card-value">${d(t)}</div>
    ${n?`<div class="card-note">${d(n)}</div>`:``}
  </div>`}function m(e){return`<span class="status status-${d(e)}">${d(e)}</span>`}var h=async e=>{let[t,n]=await Promise.all([o.exposure(`all`),o.licence()]),r=n.notice?`<div class="banner ${n.expired?`banner-bad`:`banner-warn`}">${d(n.notice)}</div>`:``;if(t.currencies.length===0){e.innerHTML=`${r}
      <h2>Outstanding</h2>
      <p class="good">No customer money is outstanding.</p>`;return}let i=t.currencies.map(e=>{let t=f([`Age`,`Transactions`,`Amount`],e.by_age.map(t=>[d(t.band.replaceAll(`_`,` `)),String(t.transactions),s(t.amount_minor,e.currency)])),n=e.unresolved.transactions>0?`<p class="note">${e.unresolved.transactions} of these are unresolved
             (${s(e.unresolved.amount_minor,e.currency)}) — we could not establish
             what happened, so they may be perfectly fine.</p>`:``;return`<section>
        <h3>${d(e.currency)}</h3>
        <div class="cards">
          ${p(`Outstanding`,s(e.amount_minor,e.currency))}
          ${p(`Customers affected`,String(e.customers_affected))}
          ${p(`Transactions`,String(e.transactions))}
          ${p(`Oldest`,`${e.oldest_age_days} days`,l(e.oldest_debit_at))}
        </div>
        ${n}
        ${t}
      </section>`}).join(``);e.innerHTML=`${r}
    <h2>Outstanding</h2>
    <p class="note">${d(t.notice)}</p>
    ${i}`},g=[`orphaned`,`reversal_pending`,`suspect`,`reversal_failed`,`pending_debit`,`completed`],_=async e=>{let t=e.dataset.status??=`orphaned`;e.innerHTML=`<h2>Transactions</h2><div class="tabs">${g.map(e=>`<button class="tab ${e===t?`tab-on`:``}" data-status="${d(e)}">${d(e.replaceAll(`_`,` `))}</button>`).join(``)}</div><div class="loading">Loading…</div>`,e.querySelectorAll(`.tab`).forEach(t=>t.addEventListener(`click`,()=>{e.dataset.status=t.dataset.status,_(e)}));let{transactions:n}=await o.transactions(t),r=f([`Transaction`,`Status`,`Amount`,`Debited`,`Detected`],n.map(e=>[`<code>${d(e.transaction_id)}</code>`,m(e.status),s(e.amount_minor,e.currency),d(l(e.debit_at)),d(l(e.detected_at))]));e.querySelector(`.loading`).outerHTML=r},v=[{id:`overview`,label:`Outstanding`,view:h},{id:`transactions`,label:`Transactions`,view:_},{id:`compliance`,label:`Compliance`,view:async e=>{let t=await o.compliance();e.innerHTML=`<h2>Reversal compliance</h2>
    ${[t.incomplete?`<div class="banner banner-bad">${d(t.notice??`This report is incomplete.`)}</div>`:``,t.truncated?`<div class="banner banner-warn">The itemised list is capped. The counts above it are exact.</div>`:``].join(``)}
    <p class="note">${d(l(t.from))} to ${d(l(t.to))},
       against a ${t.reversal_deadline_seconds}s deadline.</p>
    <div class="cards">
      ${p(`Within deadline`,String(t.compliance.within_deadline))}
      ${p(`Breached`,String(t.compliance.breached))}
      ${p(`Outstanding`,String(t.compliance.outstanding),`neither yet`)}
      ${p(`Rate`,u(t.compliance.rate),`of concluded reversals`)}
    </div>
    <div class="cards">
      ${p(`Detection p50`,c(t.detection_latency.p50_seconds))}
      ${p(`Detection p95`,c(t.detection_latency.p95_seconds))}
      ${p(`Detection max`,c(t.detection_latency.max_seconds))}
      ${p(`Samples`,String(t.detection_latency.samples))}
    </div>
    <h3>Breaches (${t.breaches.length})</h3>
    ${f([`Transaction`,`Status`,`Amount`,`Elapsed`,`Why`],t.breaches.map(e=>[`<code>${d(e.transaction_id)}</code>`,m(e.status),s(e.amount_minor,e.currency),d(c(e.elapsed_seconds)),d(e.reason)]))}
    <p class="downloads">
      <a href="/v1/reports/reversal-compliance?format=csv" data-auth-download>Download CSV</a>
      <a href="/v1/reports/reversal-compliance?format=pdf" data-auth-download>Download PDF</a>
    </p>`}},{id:`rails`,label:`Rails`,view:async e=>{let[t,n]=await Promise.all([o.scorecard(),o.windowFit()]),r=t.providers.map(e=>[d(e.provider),String(e.transactions),u(e.failure_rate)+(e.low_sample?` <span class="thin">thin sample</span>`:``),d(c(e.settlement_latency.p95_seconds)),d(e.verdict)]),i=n.rails.map(e=>[d(e.provider),`${e.window_seconds}s`,d(c(e.observed_p95_seconds)),e.recommended_window_seconds?`${e.recommended_window_seconds}s`:`—`,e.too_tight?`<span class="bad">${d(e.verdict)}</span>`:d(e.verdict)]);e.innerHTML=`<h2>Rails</h2>
    <p class="note">${d(t.scope)}</p>
    ${f([`Rail`,`Transactions`,`Failure rate`,`Settlement p95`,`Verdict`],r)}
    <h3>Window fit</h3>
    <p class="note">${d(n.notice)}</p>
    ${f([`Rail`,`Window`,`Observed p95`,`Recommended`,`Verdict`],i)}`}},{id:`audit`,label:`Audit`,view:async e=>{let[t,n]=await Promise.all([o.audit(),o.endpoints()]);e.innerHTML=`<h2>Audit</h2>
    ${t.records===0?`<div class="banner banner-warn">No audit records yet. Nothing has been
         verified because nothing has been written.</div>`:t.verified?`<div class="banner banner-good">The chain verifies. ${t.records} records.</div>`:`<div class="banner banner-bad">The chain does not verify${t.broken_at?` — broken at record ${t.broken_at}`:``}. ${d(t.reason??``)}</div>`}
    ${t.checkpoint.checked?t.checkpoint.matches?`<div class="banner banner-good">Signed at record ${t.checkpoint.seq},
         ${d(l(t.checkpoint.taken_at))}. A rewrite of the whole chain would be detected.</div>`:`<div class="banner banner-bad">${d(t.checkpoint.reason??`The signature does not match.`)}</div>`:`<div class="banner banner-warn">${d(t.checkpoint.reason??`No checkpoint has been signed.`)}</div>`}
    <h3>Delivery endpoints</h3>
    ${f([`Endpoint`,`URL`,`Events`,`Enabled`],n.endpoints.map(e=>[`<code>${d(e.id)}</code>`,`<code>${d(e.url)}</code>`,d(e.events.length?e.events.join(`, `):`all`),e.enabled?`yes`:`<span class="bad">no</span>`]))}
    ${n.endpoints.filter(e=>e.enabled).length===0?`<div class="banner banner-warn">No enabled endpoint. Reversals are detected
           and recorded, but nobody is told.</div>`:``}`}}],y=document.querySelector(`#app`);function b(e=``){y.innerHTML=`
    <div class="signin">
      <h1>ReconSync</h1>
      <p>Paste an API key to read this tenant's data.</p>
      ${e?`<div class="banner banner-bad">${e}</div>`:``}
      <form id="key-form">
        <input type="password" id="key" placeholder="rs_test_…" autocomplete="off" spellcheck="false" />
        <button type="submit">Open</button>
      </form>
      <p class="note">The key is held for this browser tab only and is cleared when you
      close it. It is sent to this same server and nowhere else.</p>
    </div>`,y.querySelector(`#key-form`).addEventListener(`submit`,e=>{e.preventDefault();let t=y.querySelector(`#key`).value.trim();t&&(r(t),x())})}function x(){let t=location.hash.slice(1)||v[0].id,n=v.find(e=>e.id===t)??v[0];y.innerHTML=`
    <header>
      <h1>ReconSync</h1>
      <nav>${v.map(e=>`<a href="#${e.id}" class="${e.id===n.id?`on`:``}">${e.label}</a>`).join(``)}</nav>
      <button id="sign-out">Sign out</button>
    </header>
    <main id="view"><div class="loading">Loading…</div></main>`,y.querySelector(`#sign-out`).addEventListener(`click`,()=>{i(),b()});let r=y.querySelector(`#view`);n.view(r).catch(t=>{if(t instanceof e&&t.status===401){i(),b(`That key was rejected.`);return}if(t instanceof e&&t.status===402){r.innerHTML=`<div class="banner banner-warn">${t.message}</div>`;return}let n=t instanceof Error?t.message:String(t);r.innerHTML=`<div class="banner banner-bad">${n}</div>`})}document.addEventListener(`click`,async e=>{let t=e.target.closest(`[data-auth-download]`);if(!t)return;e.preventDefault();let r=await fetch(t.href,{headers:{Authorization:`Bearer ${n()}`}});if(!r.ok)return;let i=await r.blob(),a=URL.createObjectURL(i),o=document.createElement(`a`);o.href=a,o.download=t.href.includes(`pdf`)?`reversal-compliance.pdf`:`reversal-compliance.csv`,o.click(),URL.revokeObjectURL(a)}),window.addEventListener(`hashchange`,()=>{n()&&x()}),n()?x():b();