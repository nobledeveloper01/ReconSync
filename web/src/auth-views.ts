// Signing in, proving a second factor, and managing an account.
//
// The password never reaches sessionStorage and the session never reaches this
// code at all — it is an HttpOnly cookie the browser holds and this script
// cannot read. What lives here is only what the person types and what the
// server chooses to tell them back.

import { ApiError, auth, clearKey, storeKey, users, type Session, type UserRow } from "./api";
import { esc } from "./format";

/** Renders a sign-in screen and resolves once the person is signed in. */
export function signIn(host: HTMLElement, onDone: (s: Session) => void, notice = ""): void {
  let email = "";
  let password = "";

  function passwordStep(message = notice, kind = "banner-bad"): void {
    host.innerHTML = `
      <div class="signin">
        <h1>ReconSync</h1>
        <p>Sign in to read this tenant's reconciliation data.</p>
        ${message ? `<div class="banner ${kind}">${esc(message)}</div>` : ""}
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
      </div>`;

    host.querySelector<HTMLAnchorElement>("#use-key")!.addEventListener("click", (e) => {
      e.preventDefault();
      keyStep(host, onDone);
    });

    host.querySelector<HTMLFormElement>("#login")!.addEventListener("submit", async (e) => {
      e.preventDefault();
      const button = host.querySelector<HTMLButtonElement>("button")!;
      button.disabled = true;
      button.textContent = "Signing in…";

      email = host.querySelector<HTMLInputElement>("#email")!.value.trim();
      password = host.querySelector<HTMLInputElement>("#password")!.value;

      try {
        const result = await auth.login(email, password);
        if ("totp_required" in result) {
          codeStep();
          return;
        }
        onDone(result);
      } catch (err) {
        passwordStep(errorText(err));
      }
    });
  }

  function codeStep(message = ""): void {
    host.innerHTML = `
      <div class="signin">
        <h1>One more step</h1>
        <p>Enter the six-digit code from your authenticator.</p>
        ${message ? `<div class="banner banner-bad">${esc(message)}</div>` : ""}
        <form id="totp">
          <input type="text" id="code" placeholder="000000" inputmode="numeric"
                 autocomplete="one-time-code" spellcheck="false" required autofocus />
          <button type="submit">Verify</button>
        </form>
        <p class="note">Lost your authenticator? Enter one of your recovery codes
        instead — each works once.</p>
        <p class="note"><a href="#" id="back">Start again</a></p>
      </div>`;

    host.querySelector<HTMLAnchorElement>("#back")!.addEventListener("click", (e) => {
      e.preventDefault();
      passwordStep("");
    });

    host.querySelector<HTMLFormElement>("#totp")!.addEventListener("submit", async (e) => {
      e.preventDefault();
      const code = host.querySelector<HTMLInputElement>("#code")!.value.trim();
      try {
        // The password is sent again rather than held in a half-authenticated
        // server-side state: there is no partial session to steal that way.
        const result = await auth.login(email, password, code);
        if ("totp_required" in result) {
          codeStep("That code did not verify.");
          return;
        }
        onDone(result);
      } catch (err) {
        codeStep(errorText(err));
      }
    });
  }

  passwordStep();
}

/** The API-key path, for a deployment with no user accounts. */
function keyStep(host: HTMLElement, onDone: (s: Session) => void, message = ""): void {
  host.innerHTML = `
    <div class="signin">
      <h1>ReconSync</h1>
      <p>Paste an API key to read this tenant's data.</p>
      ${message ? `<div class="banner banner-bad">${esc(message)}</div>` : ""}
      <form id="key-form">
        <input type="password" id="key" placeholder="rs_test_…" autocomplete="off" spellcheck="false" />
        <button type="submit">Open</button>
      </form>
      <p class="note">The key is held for this browser tab only and is cleared when you
      close it. A key has no second factor, so prefer signing in where you can.</p>
      <p class="note"><a href="#" id="use-password">Sign in with a password instead</a></p>
    </div>`;

  host.querySelector<HTMLAnchorElement>("#use-password")!.addEventListener("click", (e) => {
    e.preventDefault();
    signIn(host, onDone);
  });

  host.querySelector<HTMLFormElement>("#key-form")!.addEventListener("submit", async (e) => {
    e.preventDefault();
    const value = host.querySelector<HTMLInputElement>("#key")!.value.trim();
    if (!value) return;

    storeKey(value);
    try {
      onDone(await auth.me());
    } catch (err) {
      clearKey();
      keyStep(host, onDone, errorText(err));
    }
  });
}

/** The reset screen, reached from a link an administrator hands over. */
export function resetPassword(host: HTMLElement, token: string, onDone: () => void): void {
  function render(message = "", kind = "banner-bad"): void {
    host.innerHTML = `
      <div class="signin">
        <h1>Set a new password</h1>
        ${message ? `<div class="banner ${kind}">${esc(message)}</div>` : ""}
        <form id="reset">
          <input type="password" id="pw1" placeholder="new password"
                 autocomplete="new-password" required />
          <input type="password" id="pw2" placeholder="again"
                 autocomplete="new-password" required />
          <button type="submit">Set password</button>
        </form>
        <p class="note">At least 12 characters. Length beats symbols — a long phrase
        you can remember is stronger than a short one you cannot.</p>
      </div>`;

    host.querySelector<HTMLFormElement>("#reset")!.addEventListener("submit", async (e) => {
      e.preventDefault();
      const pw1 = host.querySelector<HTMLInputElement>("#pw1")!.value;
      const pw2 = host.querySelector<HTMLInputElement>("#pw2")!.value;
      if (pw1 !== pw2) {
        render("Those did not match.");
        return;
      }
      try {
        await auth.completeReset(token, pw1);
        onDone();
      } catch (err) {
        render(errorText(err));
      }
    });
  }
  render();
}

/** The account tab: password, second factor, and other signed-in browsers. */
export async function account(host: HTMLElement, session: Session): Promise<void> {
  host.innerHTML = `
    <h2>Your account</h2>
    <p class="sub">${esc(session.email || "API key")} —
      ${esc(session.role || "unscoped")} in ${esc(session.tenant_id)}</p>
    <div id="twofa" class="card"></div>
    <div id="password" class="card"></div>
    <div id="sessions" class="card"></div>`;

  renderTwoFactor(host.querySelector<HTMLElement>("#twofa")!, session);
  renderPassword(host.querySelector<HTMLElement>("#password")!);
  await renderSessions(host.querySelector<HTMLElement>("#sessions")!);
}

function renderTwoFactor(host: HTMLElement, session: Session): void {
  if (session.totp_enabled) {
    host.innerHTML = `
      <h3>Two-factor authentication</h3>
      <div class="banner banner-good">On. Sign-in needs a code from your authenticator.</div>
      <form id="off">
        <input type="password" id="pw" placeholder="your password" autocomplete="current-password" required />
        <button type="submit" class="danger">Turn off</button>
      </form>
      <p class="note">Your password is asked for again because turning off a second
      factor is the first thing someone who borrowed an unlocked laptop would do.</p>`;

    host.querySelector<HTMLFormElement>("#off")!.addEventListener("submit", async (e) => {
      e.preventDefault();
      try {
        await auth.disableTOTP(host.querySelector<HTMLInputElement>("#pw")!.value);
        session.totp_enabled = false;
        renderTwoFactor(host, session);
      } catch (err) {
        host.insertAdjacentHTML("beforeend", banner(errorText(err)));
      }
    });
    return;
  }

  host.innerHTML = `
    <h3>Two-factor authentication</h3>
    <div class="banner banner-warn">Off. Your password is the only thing between
    an attacker and this tenant's money movement.</div>
    <button id="start">Set up</button>`;

  host.querySelector<HTMLButtonElement>("#start")!.addEventListener("click", async () => {
    try {
      const { secret, qr } = await auth.beginTOTP();
      host.innerHTML = `
        <h3>Two-factor authentication</h3>
        <p>Scan this with your authenticator app, then confirm with a code.</p>
        ${qr ? `<div class="qr">${qr}</div>` : ""}
        <p class="note">No camera? Type this key in instead:</p>
        <p class="mono secret">${esc(secret)}</p>
        <form id="confirm">
          <input type="text" id="code" placeholder="000000" inputmode="numeric"
                 autocomplete="one-time-code" required />
          <button type="submit">Confirm</button>
        </form>
        <p class="note">Nothing changes until a code verifies — a wrong clock cannot
        lock you out of your own account here.</p>`;

      host.querySelector<HTMLFormElement>("#confirm")!.addEventListener("submit", async (e) => {
        e.preventDefault();
        try {
          const { recovery_codes } = await auth.confirmTOTP(
            host.querySelector<HTMLInputElement>("#code")!.value.trim(),
          );
          session.totp_enabled = true;
          host.innerHTML = `
            <h3>Two-factor is on</h3>
            <div class="banner banner-warn">Save these recovery codes now. Each works
            once, they are the only way in if you lose your authenticator, and they
            cannot be shown again.</div>
            <pre class="codes">${recovery_codes.map(esc).join("\n")}</pre>
            <button id="copy">Copy</button>`;
          host.querySelector<HTMLButtonElement>("#copy")!.addEventListener("click", () => {
            void navigator.clipboard.writeText(recovery_codes.join("\n"));
          });
        } catch (err) {
          host.insertAdjacentHTML("beforeend", banner(errorText(err)));
        }
      });
    } catch (err) {
      host.insertAdjacentHTML("beforeend", banner(errorText(err)));
    }
  });
}

function renderPassword(host: HTMLElement): void {
  host.innerHTML = `
    <h3>Password</h3>
    <form id="change">
      <input type="password" id="current" placeholder="current password"
             autocomplete="current-password" required />
      <input type="password" id="next" placeholder="new password (12+ characters)"
             autocomplete="new-password" required />
      <button type="submit">Change</button>
    </form>
    <p class="note">Changing it signs out every browser, including this one.</p>`;

  host.querySelector<HTMLFormElement>("#change")!.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await auth.changePassword(
        host.querySelector<HTMLInputElement>("#current")!.value,
        host.querySelector<HTMLInputElement>("#next")!.value,
      );
      location.reload();
    } catch (err) {
      host.insertAdjacentHTML("beforeend", banner(errorText(err)));
    }
  });
}

async function renderSessions(host: HTMLElement): Promise<void> {
  let rows: string;
  try {
    const { sessions } = await auth.sessions();
    rows = sessions
      .map(
        (s) => `<tr${s.current ? ' class="current"' : ""}>
          <td>${esc(s.user_agent || "unknown")}${s.current ? " <em>(this one)</em>" : ""}</td>
          <td class="mono">${esc(s.ip || "—")}</td>
          <td>${esc(new Date(s.last_seen_at).toLocaleString())}</td>
        </tr>`,
      )
      .join("");
  } catch {
    // An API key has no sessions to list, which is not an error worth shouting.
    host.innerHTML = "";
    return;
  }

  host.innerHTML = `
    <h3>Signed-in browsers</h3>
    <table><thead><tr><th>Browser</th><th>Address</th><th>Last seen</th></tr></thead>
    <tbody>${rows}</tbody></table>
    <button id="revoke" class="danger">Sign out everywhere</button>
    <p class="note">One you do not recognise is worth acting on. Signing out
    everywhere also ends this session.</p>`;

  host.querySelector<HTMLButtonElement>("#revoke")!.addEventListener("click", async () => {
    await auth.revokeSessions();
    location.reload();
  });
}

/** The users tab, shown only to an admin. */
export async function team(host: HTMLElement, session: Session): Promise<void> {
  const { users: rows } = await users.list();

  host.innerHTML = `
    <h2>Users</h2>
    <p class="sub">Who can sign in to ${esc(session.tenant_id)}.</p>
    <table>
      <thead><tr><th>Email</th><th>Role</th><th>2FA</th><th>Last seen</th><th></th></tr></thead>
      <tbody>${rows.map((u) => userRow(u, session)).join("")}</tbody>
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
    <div id="out"></div>`;

  const out = host.querySelector<HTMLElement>("#out")!;

  host.querySelector<HTMLFormElement>("#new-user")!.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await users.create(
        host.querySelector<HTMLInputElement>("#new-email")!.value.trim(),
        host.querySelector<HTMLInputElement>("#new-password")!.value,
        host.querySelector<HTMLSelectElement>("#new-role")!.value,
      );
      await team(host, session);
    } catch (err) {
      out.innerHTML = banner(errorText(err));
    }
  });

  host.querySelectorAll<HTMLSelectElement>("[data-role-for]").forEach((select) => {
    select.addEventListener("change", async () => {
      try {
        await users.update(select.dataset.roleFor!, { role: select.value });
        await team(host, session);
      } catch (err) {
        out.innerHTML = banner(errorText(err));
      }
    });
  });

  host.querySelectorAll<HTMLButtonElement>("[data-toggle-for]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await users.update(button.dataset.toggleFor!, {
          disabled: button.dataset.disabled !== "true",
        });
        await team(host, session);
      } catch (err) {
        out.innerHTML = banner(errorText(err));
      }
    });
  });

  host.querySelectorAll<HTMLButtonElement>("[data-reset-for]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        const { reset_token, expires_at } = await users.issueReset(button.dataset.resetFor!);
        const url = `${location.origin}/#reset=${encodeURIComponent(reset_token)}`;
        out.innerHTML = `
          <div class="card">
            <h3>Single-use reset link</h3>
            <p class="mono wrap">${esc(url)}</p>
            <p class="note">Hand this over on a channel you trust. It works once,
            expires ${esc(new Date(expires_at).toLocaleString())}, and any
            earlier link for them has stopped working.</p>
          </div>`;
      } catch (err) {
        out.innerHTML = banner(errorText(err));
      }
    });
  });
}

function userRow(u: UserRow, session: Session): string {
  const self = u.email === session.email;
  const options = ["viewer", "operator", "admin"]
    .map((r) => `<option value="${r}"${r === u.role ? " selected" : ""}>${r}</option>`)
    .join("");

  return `<tr class="${u.disabled ? "muted" : ""}">
    <td>${esc(u.email)}${self ? " <em>(you)</em>" : ""}</td>
    <td>${
      // Your own row is fixed: the last admin demoting themselves would leave a
      // tenant with nobody who can manage users, and the only way back is a
      // shell on the server.
      self
        ? esc(u.role)
        : `<select data-role-for="${esc(u.id)}">${options}</select>`
    }</td>
    <td>${u.totp_enabled ? "yes" : '<span class="warn">no</span>'}</td>
    <td>${u.last_login_at ? esc(new Date(u.last_login_at).toLocaleString()) : "never"}</td>
    <td class="actions">${
      self
        ? ""
        : `<button data-reset-for="${esc(u.id)}">Reset link</button>
           <button data-toggle-for="${esc(u.id)}" data-disabled="${u.disabled}"
                   class="${u.disabled ? "" : "danger"}">${u.disabled ? "Enable" : "Disable"}</button>`
    }</td>
  </tr>`;
}

function banner(message: string): string {
  return `<div class="banner banner-bad">${esc(message)}</div>`;
}

function errorText(err: unknown): string {
  if (err instanceof ApiError) return err.message;
  return err instanceof Error ? err.message : String(err);
}
