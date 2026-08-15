(function(){let e=document.createElement(`link`).relList;if(e&&e.supports&&e.supports(`modulepreload`))return;for(let e of document.querySelectorAll(`link[rel="modulepreload"]`))n(e);new MutationObserver(e=>{for(let t of e)if(t.type===`childList`)for(let e of t.addedNodes)e.tagName===`LINK`&&e.rel===`modulepreload`&&n(e)}).observe(document,{childList:!0,subtree:!0});function t(e){let t={};return e.integrity&&(t.integrity=e.integrity),e.referrerPolicy&&(t.referrerPolicy=e.referrerPolicy),t.credentials=e.crossOrigin===`use-credentials`?`include`:e.crossOrigin===`anonymous`?`omit`:`same-origin`,t}function n(e){if(e.ep)return;e.ep=!0;let n=t(e);fetch(e.href,n)}})();var e=class extends Error{status;code;constructor(e,t,n){super(n),this.status=e,this.code=t}},t=`reconsync.key`;function n(){return sessionStorage.getItem(t)}function r(e){sessionStorage.setItem(t,e)}function i(){sessionStorage.removeItem(t)}function a(){let e=document.cookie.match(/(?:^|;\s*)reconsync_csrf=([^;]*)/);return e?decodeURIComponent(e[1]):``}function o(e){let t={};e&&(t[`Content-Type`]=`application/json`);let r=a();r&&(t[`X-ReconSync-CSRF`]=r);let i=n();return i&&!r&&(t.Authorization=`Bearer ${i}`),t}async function s(t,n,r){let i=await fetch(n,{method:t,headers:o(r!==void 0),credentials:`same-origin`,body:r===void 0?void 0:JSON.stringify(r)});if(!i.ok){let t=`error`,n=`${i.status} ${i.statusText}`;try{let e=await i.json();e?.error&&(t=e.error.code??t,n=e.error.message??n)}catch{}throw new e(i.status,t,n)}if(i.status!==204)return await i.json()}function c(e){return s(`GET`,e)}function l(e,t){return e?e.scopes.length===0||e.scopes.includes(t):!1}var u={login:(e,t,n)=>s(`POST`,`/v1/auth/login`,{email:e,password:t,...n?{code:n}:{}}),me:()=>c(`/v1/auth/me`),logout:()=>s(`POST`,`/v1/auth/logout`,{}),changePassword:(e,t)=>s(`POST`,`/v1/auth/password`,{current_password:e,new_password:t}),beginTOTP:()=>s(`POST`,`/v1/auth/totp/begin`,{}),confirmTOTP:e=>s(`POST`,`/v1/auth/totp/confirm`,{code:e}),disableTOTP:e=>s(`POST`,`/v1/auth/totp/disable`,{password:e}),sessions:()=>c(`/v1/auth/sessions`),revokeSessions:()=>s(`DELETE`,`/v1/auth/sessions`),completeReset:(e,t)=>s(`POST`,`/v1/auth/reset`,{token:e,password:t})},d={list:()=>c(`/v1/users`),create:(e,t,n)=>s(`POST`,`/v1/users`,{email:e,password:t,role:n}),update:(e,t)=>s(`PATCH`,`/v1/users/${encodeURIComponent(e)}`,t),issueReset:e=>s(`POST`,`/v1/users/${encodeURIComponent(e)}/reset`,{})},f={transactions:(e,t=100)=>c(`/v1/transactions?status=${encodeURIComponent(e)}&limit=${t}`),exposure:e=>c(`/v1/reports/exposure?scope=${e}`),compliance:()=>c(`/v1/reports/reversal-compliance`),scorecard:()=>c(`/v1/reports/providers`),windowFit:()=>c(`/v1/reports/window-fit`),audit:()=>c(`/v1/audit/verify`),licence:()=>c(`/v1/licence`),endpoints:()=>c(`/v1/webhooks`)};function p(e,t){return`${t} ${e.toLocaleString(`en-US`)}`}function m(e){return e<60?`${e.toFixed(1)}s`:e<3600?`${(e/60).toFixed(1)}m`:e<86400?`${(e/3600).toFixed(1)}h`:`${(e/86400).toFixed(1)}d`}function h(e){if(!e)return`—`;let t=new Date(e);return Number.isNaN(t.getTime())?e:t.toISOString().replace(`T`,` `).slice(0,19)+`Z`}function g(e){return e==null?`—`:`${(e*100).toFixed(1)}%`}function _(e){return String(e??``).replaceAll(`&`,`&amp;`).replaceAll(`<`,`&lt;`).replaceAll(`>`,`&gt;`).replaceAll(`"`,`&quot;`).replaceAll(`'`,`&#39;`)}function v(e,t,n=``){let r=``,i=``;function a(s=n,c=`banner-bad`){e.innerHTML=`
      <div class="signin">
        <h1>ReconSync</h1>
        <p>Sign in to read this tenant's reconciliation data.</p>
        ${s?`<div class="banner ${c}">${_(s)}</div>`:``}
        <form id="login">
          <input type="email" id="email" placeholder="you@example.com"
                 autocomplete="username" spellcheck="false" required />
          <input type="password" id="password" placeholder="password"
                 autocomplete="current-password" required />
          <button type="submit">Sign in</button>
        </form>
        <p class="note">
          Forgotten it? An administrator can issue you a single-use reset link.
          If you are the administrator, run
          <code>reconsyncctl users reset-password</code> on the server.
        </p>
        <p class="note"><a href="#" id="use-key">Use an API key instead</a></p>
      </div>`,e.querySelector(`#use-key`).addEventListener(`click`,n=>{n.preventDefault(),y(e,t)}),e.querySelector(`#login`).addEventListener(`submit`,async n=>{n.preventDefault();let s=e.querySelector(`button`);s.disabled=!0,s.textContent=`Signing in…`,r=e.querySelector(`#email`).value.trim(),i=e.querySelector(`#password`).value;try{let e=await u.login(r,i);if(`totp_required`in e){o();return}t(e)}catch(e){a(O(e))}})}function o(n=``){e.innerHTML=`
      <div class="signin">
        <h1>One more step</h1>
        <p>Enter the six-digit code from your authenticator.</p>
        ${n?`<div class="banner banner-bad">${_(n)}</div>`:``}
        <form id="totp">
          <input type="text" id="code" placeholder="000000" inputmode="numeric"
                 autocomplete="one-time-code" spellcheck="false" required autofocus />
          <button type="submit">Verify</button>
        </form>
        <p class="note">Lost your authenticator? Enter one of your recovery codes
        instead — each works once.</p>
        <p class="note"><a href="#" id="back">Start again</a></p>
      </div>`,e.querySelector(`#back`).addEventListener(`click`,e=>{e.preventDefault(),a(``)}),e.querySelector(`#totp`).addEventListener(`submit`,async n=>{n.preventDefault();let a=e.querySelector(`#code`).value.trim();try{let e=await u.login(r,i,a);if(`totp_required`in e){o(`That code did not verify.`);return}t(e)}catch(e){o(O(e))}})}a()}function y(e,t,n=``){e.innerHTML=`
    <div class="signin">
      <h1>ReconSync</h1>
      <p>Paste an API key to read this tenant's data.</p>
      ${n?`<div class="banner banner-bad">${_(n)}</div>`:``}
      <form id="key-form">
        <input type="password" id="key" placeholder="rs_test_…" autocomplete="off" spellcheck="false" />
        <button type="submit">Open</button>
      </form>
      <p class="note">The key is held for this browser tab only and is cleared when you
      close it. A key has no second factor, so prefer signing in where you can.</p>
      <p class="note"><a href="#" id="use-password">Sign in with a password instead</a></p>
    </div>`,e.querySelector(`#use-password`).addEventListener(`click`,n=>{n.preventDefault(),v(e,t)}),e.querySelector(`#key-form`).addEventListener(`submit`,async n=>{n.preventDefault();let a=e.querySelector(`#key`).value.trim();if(a){r(a);try{t(await u.me())}catch(n){i(),y(e,t,O(n))}}})}function b(e,t,n){function r(i=``,a=`banner-bad`){e.innerHTML=`
      <div class="signin">
        <h1>Set a new password</h1>
        ${i?`<div class="banner ${a}">${_(i)}</div>`:``}
        <form id="reset">
          <input type="password" id="pw1" placeholder="new password"
                 autocomplete="new-password" required />
          <input type="password" id="pw2" placeholder="again"
                 autocomplete="new-password" required />
          <button type="submit">Set password</button>
        </form>
        <p class="note">At least 12 characters. Length beats symbols — a long phrase
        you can remember is stronger than a short one you cannot.</p>
      </div>`,e.querySelector(`#reset`).addEventListener(`submit`,async i=>{i.preventDefault();let a=e.querySelector(`#pw1`).value;if(a!==e.querySelector(`#pw2`).value){r(`Those did not match.`);return}try{await u.completeReset(t,a),n()}catch(e){r(O(e))}})}r()}async function x(e,t){e.innerHTML=`
    <h2>Your account</h2>
    <p class="sub">${_(t.email||`API key`)} —
      ${_(t.role||`unscoped`)} in ${_(t.tenant_id)}</p>
    <div id="twofa" class="card"></div>
    <div id="password" class="card"></div>
    <div id="sessions" class="card"></div>`,S(e.querySelector(`#twofa`),t),C(e.querySelector(`#password`)),await w(e.querySelector(`#sessions`))}function S(e,t){if(t.totp_enabled){e.innerHTML=`
      <h3>Two-factor authentication</h3>
      <div class="banner banner-good">On. Sign-in needs a code from your authenticator.</div>
      <form id="off">
        <input type="password" id="pw" placeholder="your password" autocomplete="current-password" required />
        <button type="submit" class="danger">Turn off</button>
      </form>
      <p class="note">Your password is asked for again because turning off a second
      factor is the first thing someone who borrowed an unlocked laptop would do.</p>`,e.querySelector(`#off`).addEventListener(`submit`,async n=>{n.preventDefault();try{await u.disableTOTP(e.querySelector(`#pw`).value),t.totp_enabled=!1,S(e,t)}catch(t){e.insertAdjacentHTML(`beforeend`,D(O(t)))}});return}e.innerHTML=`
    <h3>Two-factor authentication</h3>
    <div class="banner banner-warn">Off. Your password is the only thing between
    an attacker and this tenant's money movement.</div>
    <button id="start">Set up</button>`,e.querySelector(`#start`).addEventListener(`click`,async()=>{try{let{secret:n,qr:r}=await u.beginTOTP();e.innerHTML=`
        <h3>Two-factor authentication</h3>
        <p>Scan this with your authenticator app, then confirm with a code.</p>
        ${r?`<div class="qr">${r}</div>`:``}
        <p class="note">No camera? Type this key in instead:</p>
        <p class="mono secret">${_(n)}</p>
        <form id="confirm">
          <input type="text" id="code" placeholder="000000" inputmode="numeric"
                 autocomplete="one-time-code" required />
          <button type="submit">Confirm</button>
        </form>
        <p class="note">Nothing changes until a code verifies — a wrong clock cannot
        lock you out of your own account here.</p>`,e.querySelector(`#confirm`).addEventListener(`submit`,async n=>{n.preventDefault();try{let{recovery_codes:n}=await u.confirmTOTP(e.querySelector(`#code`).value.trim());t.totp_enabled=!0,e.innerHTML=`
            <h3>Two-factor is on</h3>
            <div class="banner banner-warn">Save these recovery codes now. Each works
            once, they are the only way in if you lose your authenticator, and they
            cannot be shown again.</div>
            <pre class="codes">${n.map(_).join(`
`)}</pre>
            <button id="copy">Copy</button>`,e.querySelector(`#copy`).addEventListener(`click`,()=>{navigator.clipboard.writeText(n.join(`
`))})}catch(t){e.insertAdjacentHTML(`beforeend`,D(O(t)))}})}catch(t){e.insertAdjacentHTML(`beforeend`,D(O(t)))}})}function C(e){e.innerHTML=`
    <h3>Password</h3>
    <form id="change">
      <input type="password" id="current" placeholder="current password"
             autocomplete="current-password" required />
      <input type="password" id="next" placeholder="new password (12+ characters)"
             autocomplete="new-password" required />
      <button type="submit">Change</button>
    </form>
    <p class="note">Changing it signs out every browser, including this one.</p>`,e.querySelector(`#change`).addEventListener(`submit`,async t=>{t.preventDefault();try{await u.changePassword(e.querySelector(`#current`).value,e.querySelector(`#next`).value),location.reload()}catch(t){e.insertAdjacentHTML(`beforeend`,D(O(t)))}})}async function w(e){let t;try{let{sessions:e}=await u.sessions();t=e.map(e=>`<tr${e.current?` class="current"`:``}>
          <td>${_(e.user_agent||`unknown`)}${e.current?` <em>(this one)</em>`:``}</td>
          <td class="mono">${_(e.ip||`—`)}</td>
          <td>${_(new Date(e.last_seen_at).toLocaleString())}</td>
        </tr>`).join(``)}catch{e.innerHTML=``;return}e.innerHTML=`
    <h3>Signed-in browsers</h3>
    <table><thead><tr><th>Browser</th><th>Address</th><th>Last seen</th></tr></thead>
    <tbody>${t}</tbody></table>
    <button id="revoke" class="danger">Sign out everywhere</button>
    <p class="note">One you do not recognise is worth acting on. Signing out
    everywhere also ends this session.</p>`,e.querySelector(`#revoke`).addEventListener(`click`,async()=>{await u.revokeSessions(),location.reload()})}async function T(e,t){let{users:n}=await d.list();e.innerHTML=`
    <h2>Users</h2>
    <p class="sub">Who can sign in to ${_(t.tenant_id)}.</p>
    <table>
      <thead><tr><th>Email</th><th>Role</th><th>2FA</th><th>Last seen</th><th></th></tr></thead>
      <tbody>${n.map(e=>E(e,t)).join(``)}</tbody>
    </table>
    <div class="card">
      <h3>Add someone</h3>
      <form id="new-user">
        <input type="email" id="new-email" placeholder="them@example.com" required />
        <input type="password" id="new-password" placeholder="temporary password (12+)"
               autocomplete="new-password" required />
        <select id="new-role">
          <option value="viewer">Viewer — reads reports</option>
          <option value="operator">Operator — also acts on transactions</option>
          <option value="admin">Admin — also manages endpoints and users</option>
        </select>
        <button type="submit">Create</button>
      </form>
      <p class="note">Give them the temporary password over a channel you trust, and
      tell them to change it and enrol a second factor.</p>
    </div>
    <div id="out"></div>`;let r=e.querySelector(`#out`);e.querySelector(`#new-user`).addEventListener(`submit`,async n=>{n.preventDefault();try{await d.create(e.querySelector(`#new-email`).value.trim(),e.querySelector(`#new-password`).value,e.querySelector(`#new-role`).value),await T(e,t)}catch(e){r.innerHTML=D(O(e))}}),e.querySelectorAll(`[data-role-for]`).forEach(n=>{n.addEventListener(`change`,async()=>{try{await d.update(n.dataset.roleFor,{role:n.value}),await T(e,t)}catch(e){r.innerHTML=D(O(e))}})}),e.querySelectorAll(`[data-toggle-for]`).forEach(n=>{n.addEventListener(`click`,async()=>{try{await d.update(n.dataset.toggleFor,{disabled:n.dataset.disabled!==`true`}),await T(e,t)}catch(e){r.innerHTML=D(O(e))}})}),e.querySelectorAll(`[data-reset-for]`).forEach(e=>{e.addEventListener(`click`,async()=>{try{let{reset_token:t,expires_at:n}=await d.issueReset(e.dataset.resetFor),i=`${location.origin}/#reset=${encodeURIComponent(t)}`;r.innerHTML=`
          <div class="card">
            <h3>Single-use reset link</h3>
            <p class="mono wrap">${_(i)}</p>
            <p class="note">Hand this over on a channel you trust. It works once,
            expires ${_(new Date(n).toLocaleString())}, and any
            earlier link for them has stopped working.</p>
          </div>`}catch(e){r.innerHTML=D(O(e))}})})}function E(e,t){let n=e.email===t.email,r=[`viewer`,`operator`,`admin`].map(t=>`<option value="${t}"${t===e.role?` selected`:``}>${t}</option>`).join(``);return`<tr class="${e.disabled?`muted`:``}">
    <td>${_(e.email)}${n?` <em>(you)</em>`:``}</td>
    <td>${n?_(e.role):`<select data-role-for="${_(e.id)}">${r}</select>`}</td>
    <td>${e.totp_enabled?`yes`:`<span class="warn">no</span>`}</td>
    <td>${e.last_login_at?_(new Date(e.last_login_at).toLocaleString()):`never`}</td>
    <td class="actions">${n?``:`<button data-reset-for="${_(e.id)}">Reset link</button>
           <button data-toggle-for="${_(e.id)}" data-disabled="${e.disabled}"
                   class="${e.disabled?``:`danger`}">${e.disabled?`Enable`:`Disable`}</button>`}</td>
  </tr>`}function D(e){return`<div class="banner banner-bad">${_(e)}</div>`}function O(t){return t instanceof e||t instanceof Error?t.message:String(t)}function k(e,t){return t.length===0?`<p class="empty">Nothing to show.</p>`:`<table>
    <thead><tr>${e.map(e=>`<th>${_(e)}</th>`).join(``)}</tr></thead>
    <tbody>${t.map(e=>`<tr>${e.map(e=>`<td>${e}</td>`).join(``)}</tr>`).join(``)}</tbody>
  </table>`}function A(e,t,n=``){return`<div class="card">
    <div class="card-title">${_(e)}</div>
    <div class="card-value">${_(t)}</div>
    ${n?`<div class="card-note">${_(n)}</div>`:``}
  </div>`}function j(e){return`<span class="status status-${_(e)}">${_(e)}</span>`}var M=async e=>{let[t,n]=await Promise.all([f.exposure(`all`),f.licence()]),r=n.notice?`<div class="banner ${n.expired?`banner-bad`:`banner-warn`}">${_(n.notice)}</div>`:``;if(t.currencies.length===0){e.innerHTML=`${r}
      <h2>Outstanding</h2>
      <p class="good">No customer money is outstanding.</p>`;return}let i=t.currencies.map(e=>{let t=k([`Age`,`Transactions`,`Amount`],e.by_age.map(t=>[_(t.band.replaceAll(`_`,` `)),String(t.transactions),p(t.amount_minor,e.currency)])),n=e.unresolved.transactions>0?`<p class="note">${e.unresolved.transactions} of these are unresolved
             (${p(e.unresolved.amount_minor,e.currency)}) — we could not establish
             what happened, so they may be perfectly fine.</p>`:``;return`<section>
        <h3>${_(e.currency)}</h3>
        <div class="cards">
          ${A(`Outstanding`,p(e.amount_minor,e.currency))}
          ${A(`Customers affected`,String(e.customers_affected))}
          ${A(`Transactions`,String(e.transactions))}
          ${A(`Oldest`,`${e.oldest_age_days} days`,h(e.oldest_debit_at))}
        </div>
        ${n}
        ${t}
      </section>`}).join(``);e.innerHTML=`${r}
    <h2>Outstanding</h2>
    <p class="note">${_(t.notice)}</p>
    ${i}`},N=[`orphaned`,`reversal_pending`,`suspect`,`reversal_failed`,`pending_debit`,`completed`],P=async e=>{let t=e.dataset.status??=`orphaned`;e.innerHTML=`<h2>Transactions</h2><div class="tabs">${N.map(e=>`<button class="tab ${e===t?`tab-on`:``}" data-status="${_(e)}">${_(e.replaceAll(`_`,` `))}</button>`).join(``)}</div><div class="loading">Loading…</div>`,e.querySelectorAll(`.tab`).forEach(t=>t.addEventListener(`click`,()=>{e.dataset.status=t.dataset.status,P(e)}));let{transactions:n}=await f.transactions(t),r=k([`Transaction`,`Status`,`Amount`,`Debited`,`Detected`],n.map(e=>[`<code>${_(e.transaction_id)}</code>`,j(e.status),p(e.amount_minor,e.currency),_(h(e.debit_at)),_(h(e.detected_at))]));e.querySelector(`.loading`).outerHTML=r},F=[{id:`overview`,label:`Outstanding`,view:M,scope:`reports:read`},{id:`transactions`,label:`Transactions`,view:P,scope:`reports:read`},{id:`compliance`,label:`Compliance`,view:async e=>{let t=await f.compliance();e.innerHTML=`<h2>Reversal compliance</h2>
    ${[t.incomplete?`<div class="banner banner-bad">${_(t.notice??`This report is incomplete.`)}</div>`:``,t.truncated?`<div class="banner banner-warn">The itemised list is capped. The counts above it are exact.</div>`:``].join(``)}
    <p class="note">${_(h(t.from))} to ${_(h(t.to))},
       against a ${t.reversal_deadline_seconds}s deadline.</p>
    <div class="cards">
      ${A(`Within deadline`,String(t.compliance.within_deadline))}
      ${A(`Breached`,String(t.compliance.breached))}
      ${A(`Outstanding`,String(t.compliance.outstanding),`neither yet`)}
      ${A(`Rate`,g(t.compliance.rate),`of concluded reversals`)}
    </div>
    <div class="cards">
      ${A(`Detection p50`,m(t.detection_latency.p50_seconds))}
      ${A(`Detection p95`,m(t.detection_latency.p95_seconds))}
      ${A(`Detection max`,m(t.detection_latency.max_seconds))}
      ${A(`Samples`,String(t.detection_latency.samples))}
    </div>
    <h3>Breaches (${t.breaches.length})</h3>
    ${k([`Transaction`,`Status`,`Amount`,`Elapsed`,`Why`],t.breaches.map(e=>[`<code>${_(e.transaction_id)}</code>`,j(e.status),p(e.amount_minor,e.currency),_(m(e.elapsed_seconds)),_(e.reason)]))}
    <p class="downloads">
      <a href="/v1/reports/reversal-compliance?format=csv" data-auth-download>Download CSV</a>
      <a href="/v1/reports/reversal-compliance?format=pdf" data-auth-download>Download PDF</a>
    </p>`},scope:`reports:read`},{id:`rails`,label:`Rails`,view:async e=>{let[t,n]=await Promise.all([f.scorecard(),f.windowFit()]),r=t.providers.map(e=>[_(e.provider),String(e.transactions),g(e.failure_rate)+(e.low_sample?` <span class="thin">thin sample</span>`:``),_(m(e.settlement_latency.p95_seconds)),_(e.verdict)]),i=n.rails.map(e=>[_(e.provider),`${e.window_seconds}s`,_(m(e.observed_p95_seconds)),e.recommended_window_seconds?`${e.recommended_window_seconds}s`:`—`,e.too_tight?`<span class="bad">${_(e.verdict)}</span>`:_(e.verdict)]);e.innerHTML=`<h2>Rails</h2>
    <p class="note">${_(t.scope)}</p>
    ${k([`Rail`,`Transactions`,`Failure rate`,`Settlement p95`,`Verdict`],r)}
    <h3>Window fit</h3>
    <p class="note">${_(n.notice)}</p>
    ${k([`Rail`,`Window`,`Observed p95`,`Recommended`,`Verdict`],i)}`},scope:`reports:read`},{id:`audit`,label:`Audit`,view:async e=>{let[t,n]=await Promise.all([f.audit(),f.endpoints()]);e.innerHTML=`<h2>Audit</h2>
    ${t.records===0?`<div class="banner banner-warn">No audit records yet. Nothing has been
         verified because nothing has been written.</div>`:t.verified?`<div class="banner banner-good">The chain verifies. ${t.records} records.</div>`:`<div class="banner banner-bad">The chain does not verify${t.broken_at?` — broken at record ${t.broken_at}`:``}. ${_(t.reason??``)}</div>`}
    ${t.checkpoint.checked?t.checkpoint.matches?`<div class="banner banner-good">Signed at record ${t.checkpoint.seq},
         ${_(h(t.checkpoint.taken_at))}. A rewrite of the whole chain would be detected.</div>`:`<div class="banner banner-bad">${_(t.checkpoint.reason??`The signature does not match.`)}</div>`:`<div class="banner banner-warn">${_(t.checkpoint.reason??`No checkpoint has been signed.`)}</div>`}
    <h3>Delivery endpoints</h3>
    ${k([`Endpoint`,`URL`,`Events`,`Enabled`],n.endpoints.map(e=>[`<code>${_(e.id)}</code>`,`<code>${_(e.url)}</code>`,_(e.events.length?e.events.join(`, `):`all`),e.enabled?`yes`:`<span class="bad">no</span>`]))}
    ${n.endpoints.filter(e=>e.enabled).length===0?`<div class="banner banner-warn">No enabled endpoint. Reversals are detected
           and recorded, but nobody is told.</div>`:``}`},scope:`reports:read`},{id:`team`,label:`Users`,view:async()=>{},adminOnly:!0},{id:`account`,label:`Account`,view:async()=>{}}],I=document.querySelector(`#app`),L=null;function R(){return F.filter(e=>e.adminOnly?l(L,`endpoints:write`):!e.scope||l(L,e.scope))}function z(){let t=R(),n=location.hash.slice(1)||t[0].id,r=t.find(e=>e.id===n)??t[0];I.innerHTML=`
    <header>
      <h1>ReconSync</h1>
      <nav>${t.map(e=>`<a href="#${e.id}" class="${e.id===r.id?`on`:``}">${_(e.label)}</a>`).join(``)}</nav>
      <div class="who">
        <span class="whoami">${_(L?.email||`API key`)}${L?.role?` · ${_(L.role)}`:``}</span>
        <button id="sign-out">Sign out</button>
      </div>
    </header>
    <main id="view"><div class="loading">Loading…</div></main>`,I.querySelector(`#sign-out`).addEventListener(`click`,async()=>{i();try{await u.logout()}catch{}L=null,location.hash=``,V(`You are signed out.`,`banner-good`)});let a=I.querySelector(`#view`);(r.id===`account`?x(a,L):r.id===`team`?T(a,L):r.view(a)).catch(t=>{if(t instanceof e&&t.status===401){i(),L=null,V(`Your session ended. Sign in again.`);return}if(t instanceof e&&t.status===403){a.innerHTML=`<div class="banner banner-warn">${_(t.message)}</div>`;return}if(t instanceof e&&t.status===402){a.innerHTML=`<div class="banner banner-warn">${_(t.message)}</div>`;return}let n=t instanceof Error?t.message:String(t);a.innerHTML=`<div class="banner banner-bad">${_(n)}</div>`})}function B(e){L=e,!e.totp_enabled&&e.email&&(location.hash=location.hash||`#account`),z()}function V(e=``,t=`banner-bad`){v(I,B,e),e&&t!==`banner-bad`&&I.querySelector(`.banner`)?.classList.replace(`banner-bad`,t)}document.addEventListener(`click`,async e=>{let t=e.target.closest(`[data-auth-download]`);if(!t)return;e.preventDefault();let r=n(),i=await fetch(t.href,{credentials:`same-origin`,headers:r?{Authorization:`Bearer ${r}`}:{}});if(!i.ok)return;let a=await i.blob(),o=URL.createObjectURL(a),s=document.createElement(`a`);s.href=o,s.download=t.href.includes(`pdf`)?`reversal-compliance.pdf`:`reversal-compliance.csv`,s.click(),URL.revokeObjectURL(o)}),window.addEventListener(`hashchange`,()=>{L&&z()});var H=location.hash.match(/^#reset=(.+)$/);H?b(I,decodeURIComponent(H[1]),()=>{location.hash=``,V(`Password set. Sign in with it.`,`banner-good`)}):u.me().then(B).catch(()=>V());