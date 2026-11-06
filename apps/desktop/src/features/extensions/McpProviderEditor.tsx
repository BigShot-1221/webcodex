import { useRef, useState, type FormEvent } from "react";
import { desktopApi } from "../../lib/desktop-api";
import type { DesktopState } from "../../models/topology";
import type { McpProviderProfile, McpProviderRequest, McpProviderTransport } from "../../models/connections-tools";
import { useProduct } from "../../i18n/product";
import { useConnectionsTools } from "../../i18n/connections-tools";
import { WorkspaceDialog } from "../workspace/WorkspaceDialog";

type EnvField = { row: number; name: string; value: string; savedName: string | null };

const FORBIDDEN_HTTP_HEADERS = new Set([
  "connection", "keep-alive", "proxy-connection", "proxy-authorization", "proxy-authenticate",
  "transfer-encoding", "upgrade", "te", "trailer", "trailers", "host", "content-length", "content-type",
  "accept", "expect", "last-event-id", "mcp-session-id", "mcp-protocol-version", "mcp-method", "mcp-name",
]);

function parseStringMap(raw: string): Record<string, string> {
  const parsed: unknown = JSON.parse(raw);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("invalid");
  const entries: [string, string][] = [];
  for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof key !== "string" || typeof value !== "string" || key.includes("\0") || value.includes("\0")) throw new Error("invalid");
    entries.push([key, value]);
  }
  if (entries.length > 32) throw new Error("invalid");
  return Object.fromEntries(entries);
}

function streamableUrlAuthorityHasUserinfo(value: string): boolean {
  const schemeEnd = value.indexOf("://");
  if (schemeEnd === -1) return false;
  const rest = value.slice(schemeEnd + 3);
  const authorityEnd = rest.search(/[/?#]/);
  const authority = authorityEnd === -1 ? rest : rest.slice(0, authorityEnd);
  return authority.includes("@");
}

function validateHttpHeaderName(name: string): boolean {
  return name.length > 0 && name.length <= 128 && !name.includes("\0") && !FORBIDDEN_HTTP_HEADERS.has(name.toLowerCase())
    && /^[!#$%&'*+\-.0-9A-Z^_`a-z|~]+$/.test(name);
}

function validateQueryName(name: string): boolean {
  return name.length > 0 && name.length <= 128 && !name.includes("\0") && !/[=&]/.test(name)
    && /^[A-Za-z0-9._~-]+$/.test(name);
}

function validateMappings(
  mappings: Record<string, string>,
  envKeys: Set<string>,
  headers: boolean,
): boolean {
  const seen = new Set<string>();
  for (const [target, source] of Object.entries(mappings)) {
    if (headers ? !validateHttpHeaderName(target) : !validateQueryName(target)) return false;
    if (!/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(source) || !envKeys.has(source)) return false;
    const normalized = headers ? target.toLowerCase() : target;
    if (seen.has(normalized)) return false;
    seen.add(normalized);
  }
  return true;
}

function validateStreamableUrl(value: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    return false;
  }
  if (value.includes("#") || streamableUrlAuthorityHasUserinfo(value)) return false;
  if (parsed.username || parsed.password) return false;
  if (value.split("#", 1)[0].includes("?")) return false;
  if (parsed.port === "0") return false;
  if (!parsed.pathname.startsWith("/")) return false;
  if (parsed.protocol === "https:") return true;
  if (parsed.protocol !== "http:") return false;
  const host = parsed.hostname.toLowerCase().replace(/\.$/, "");
  return host === "localhost" || host.endsWith(".localhost") || host === "[::1]" || /^127\.\d+\.\d+\.\d+$/.test(host);
}

export function McpProviderEditor({ profile, revision, onState, onClose }: { profile: McpProviderProfile | null; revision: number; onState: (state: DesktopState) => void; onClose: () => void }) {
  const p = useProduct(); const c = useConnectionsTools();
  const initialTransport: McpProviderTransport = profile?.transport ?? "stdio";
  const [name, setName] = useState(profile?.name ?? "");
  const [transport, setTransport] = useState<McpProviderTransport>(initialTransport);
  const [command, setCommand] = useState(profile?.command ?? "");
  const [args, setArgs] = useState(JSON.stringify(profile?.args ?? []));
  const [cwd, setCwd] = useState(profile?.cwd ?? "");
  const [url, setUrl] = useState(profile?.url ?? "");
  const [httpHeaders, setHttpHeaders] = useState(JSON.stringify(profile?.http_headers ?? {}, null, 2));
  const [httpQueryParams, setHttpQueryParams] = useState(JSON.stringify(profile?.http_query_params ?? {}, null, 2));
  const [enabled, setEnabled] = useState(profile?.enabled ?? true);
  const [env, setEnv] = useState<EnvField[]>(() => (profile?.env_keys ?? []).map((name, row) => ({ row, name, value: "", savedName: name })));
  const nextRow = useRef(env.length); const submitting = useRef(false);
  const [busy, setBusy] = useState(false); const [error, setError] = useState<"invalidFields" | "operationFailed" | null>(null);
  const changeEnv = (row: number, patch: Partial<EnvField>) => setEnv(fields => fields.map(field => field.row === row ? { ...field, ...patch } : field));
  const submit = async (event: FormEvent) => {
    event.preventDefault(); if (submitting.current) return;
    let request: McpProviderRequest;
    try {
      const keys = new Set<string>();
      const entries = env.map(field => {
        const key = field.name.trim(); const normalized = key.toUpperCase();
        if (!/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(key) || normalized.startsWith("WEBCODEX_") || normalized === "AUTHORIZATION" || keys.has(normalized)) throw new Error("invalid");
        keys.add(normalized);
        return [key, !field.value && field.savedName === key ? null : field.value] as const;
      });
      const envKeySet = new Set(entries.map(([key]) => key));
      if (transport === "stdio") {
        const parsed: unknown = JSON.parse(args);
        if (!Array.isArray(parsed) || parsed.length > 64 || parsed.some(value => typeof value !== "string" || value.includes("\0") || value.length > 4096)) throw new Error("invalid");
        if (!command.trim()) throw new Error("invalid");
        request = {
          id: profile?.id ?? null, expected_revision: revision, name: name.trim(), transport, command: command.trim(),
          args: parsed as string[], cwd: cwd.trim() || null, enabled, env: Object.fromEntries(entries),
          url: null, http_headers: {}, http_query_params: {},
        };
      } else {
        const endpoint = url.trim();
        if (!validateStreamableUrl(endpoint)) throw new Error("invalid");
        const headers = parseStringMap(httpHeaders);
        const query = parseStringMap(httpQueryParams);
        if (!validateMappings(headers, envKeySet, true) || !validateMappings(query, envKeySet, false)) throw new Error("invalid");
        const mappedEnvKeys = new Set([...Object.values(headers), ...Object.values(query)]);
        if (entries.some(([key, value]) => mappedEnvKeys.has(key) && value !== null && (value.length === 0 || /[\u0000-\u001F\u007F-\u009F]/.test(value)))) throw new Error("invalid");
        request = {
          id: profile?.id ?? null, expected_revision: revision, name: name.trim(), transport: "streamable_http",
          command: "", args: [], cwd: null, url: endpoint, http_headers: headers, http_query_params: query,
          enabled, env: Object.fromEntries(entries),
        };
      }
    } catch { setError("invalidFields"); return; }
    submitting.current = true; setBusy(true); setError(null);
    setEnv(fields => fields.map(field => ({ ...field, value: "" })));
    try { onState(await desktopApi.saveMcpProvider(request)); onClose(); }
    catch { setError("operationFailed"); }
    finally { request.env = {}; submitting.current = false; setBusy(false); }
  };
  const isStdio = transport === "stdio";
  return <WorkspaceDialog title={profile ? `${p("edit")} ${profile.name}` : c("addMcpProvider")} onClose={onClose} busy={busy}>
    <form className="profile-editor" onSubmit={event => void submit(event)} aria-busy={busy}>
      <div className="field-group"><label htmlFor="mcp-provider-name">{c("providerName")}</label><input id="mcp-provider-name" value={name} onChange={event => setName(event.target.value)} maxLength={128} required disabled={busy} autoFocus /></div>
      <fieldset className="field-group" disabled={busy}><legend>{c("mcpTransport")}</legend>
        <label className="profile-checkbox"><input type="radio" name="mcp-transport" checked={isStdio} onChange={() => setTransport("stdio")} />{c("mcpTransportStdio")}</label>
        <label className="profile-checkbox"><input type="radio" name="mcp-transport" checked={!isStdio} onChange={() => setTransport("streamable_http")} />{c("mcpTransportStreamableHttp")}</label>
      </fieldset>
      {isStdio ? <>
        <div className="field-group"><label htmlFor="mcp-provider-command">{c("command")}</label><input id="mcp-provider-command" value={command} onChange={event => setCommand(event.target.value)} maxLength={1024} required disabled={busy} spellCheck={false} placeholder="npx" /></div>
        <div className="field-group"><label htmlFor="mcp-provider-arguments">{c("arguments")}</label><textarea id="mcp-provider-arguments" value={args} onChange={event => setArgs(event.target.value)} maxLength={65536} required rows={3} disabled={busy} spellCheck={false} aria-describedby="mcp-arguments-help" /><small id="mcp-arguments-help">{c("argsHelp")}</small></div>
        <details><summary>{p("advanced")}</summary><div className="field-group"><label htmlFor="mcp-provider-cwd">{c("workingDirectory")}</label><input id="mcp-provider-cwd" value={cwd} onChange={event => setCwd(event.target.value)} maxLength={4096} disabled={busy} spellCheck={false} /></div></details>
      </> : <>
        <div className="field-group"><label htmlFor="mcp-provider-url">{c("mcpEndpointUrl")}</label><input id="mcp-provider-url" value={url} onChange={event => setUrl(event.target.value)} maxLength={4096} required disabled={busy} spellCheck={false} placeholder="https://example.com/mcp" aria-describedby="mcp-url-help" /><small id="mcp-url-help">{c("mcpEndpointUrlHelp")}</small></div>
        <div className="field-group"><label htmlFor="mcp-provider-http-headers">{c("mcpHttpHeaders")}</label><textarea id="mcp-provider-http-headers" value={httpHeaders} onChange={event => setHttpHeaders(event.target.value)} maxLength={65536} required rows={4} disabled={busy} spellCheck={false} aria-describedby="mcp-http-headers-help" /><small id="mcp-http-headers-help">{c("mcpHttpHeadersHelp")}</small></div>
        <div className="field-group"><label htmlFor="mcp-provider-http-query">{c("mcpHttpQueryParams")}</label><textarea id="mcp-provider-http-query" value={httpQueryParams} onChange={event => setHttpQueryParams(event.target.value)} maxLength={65536} required rows={4} disabled={busy} spellCheck={false} aria-describedby="mcp-http-query-help" /><small id="mcp-http-query-help">{c("mcpHttpQueryHelp")}</small></div>
      </>}
      <label className="profile-checkbox" htmlFor="mcp-provider-enabled"><input id="mcp-provider-enabled" type="checkbox" checked={enabled} onChange={event => setEnabled(event.target.checked)} disabled={busy} />{c("enabled")}</label>
      <fieldset className="mcp-environment" disabled={busy}><legend>{c("environment")}</legend>
        {env.map(field => <div className="mcp-environment-row" key={field.row}>
          <div className="field-group"><label htmlFor={`mcp-env-name-${field.row}`}>{c("envName")} {field.row + 1}</label><input id={`mcp-env-name-${field.row}`} value={field.name} onChange={event => changeEnv(field.row, { name: event.target.value })} maxLength={128} required spellCheck={false} /></div>
          <div className="field-group"><label htmlFor={`mcp-env-value-${field.row}`}>{c("envValue")} {field.name || field.row + 1}</label><input id={`mcp-env-value-${field.row}`} type="password" value={field.value} onChange={event => changeEnv(field.row, { value: event.target.value })} maxLength={8192} autoComplete="new-password" spellCheck={false} aria-describedby={field.savedName ? `mcp-env-retained-${field.row}` : undefined} />{field.savedName && <small id={`mcp-env-retained-${field.row}`}>{c("keepCredential")}</small>}</div>
          <button type="button" className="text-button" aria-label={`${c("remove")} ${field.name || field.row + 1}`} onClick={() => setEnv(fields => fields.filter(value => value.row !== field.row))}>{c("remove")}</button>
        </div>)}
        <button type="button" className="secondary-button" disabled={env.length >= 64} onClick={() => { const row = nextRow.current++; setEnv(fields => [...fields, { row, name: "", value: "", savedName: null }]); }}>{c("addEnvironment")}</button>
      </fieldset>
      {error && <p role="alert" className="workspace-notice">{c(error)}</p>}
      <div className="connection-actions"><button className="primary-button" type="submit" disabled={busy}>{p("save")}</button><button className="secondary-button" type="button" disabled={busy} onClick={onClose}>{p("cancel")}</button></div>
    </form>
  </WorkspaceDialog>;
}
