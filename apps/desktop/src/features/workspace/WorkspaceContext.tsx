import { createContext, useCallback, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { invoke } from "@tauri-apps/api/core";
import type { DesktopState } from "../../models/topology";
import type { RunnerOverview, ServerRunnerSummary, ServerOverview, ServerProjects, WindowSummary, WorkspaceProject, WorkspaceRequest, WorkflowSession } from "../../models/workspace";

export function workspaceQuery<T>(request: WorkspaceRequest): Promise<T> {
  return invoke<T>("workspace_query", { request });
}
// UI fallback only; authoritative runtime IDs take precedence.
export function sameProjectPath(a?: string, b?: string): boolean {
  if (!a || !b) return false;
  const normalize = (value: string) => {
    const windows = /^[A-Za-z]:[\\/]/.test(value) || value.startsWith("\\\\");
    if (windows) {
      value = value.replace(/^\\\\\?\\UNC\\/i, "\\\\").replace(/^\\\\\?\\(?=[a-z]:\\)/i, "");
      return value.replace(/\\/g, "/").replace(/\/+$/, "").toLowerCase();
    }
    return value.replace(/\/+$/, "") || "/";
  };
  return normalize(a) === normalize(b);
}
export function sameProject(row: WorkspaceProject, saved: { runtime_project_id?: string | null; path?: string }, localRunner?: string): boolean {
  if (!saved.runtime_project_id && localRunner && row.id && !row.id.startsWith(`agent:${localRunner}:`)) return false;
  return row.id && saved.runtime_project_id
    ? row.id === saved.runtime_project_id
    : sameProjectPath(row.path, saved.path);
}
export function mergeProjects(runner: WorkspaceProject[], saved: { runtime_project_id?: string | null; path: string }[], complete = false, localRunner?: string): WorkspaceProject[] {
  const rows = [...runner];
  for (const project of complete ? [] : saved) {
    if (!rows.some(row => sameProject(row, project, localRunner))) rows.push({
      id: project.runtime_project_id || "", path: project.path, connected: false,
    });
  }
  return rows.sort((a, b) => (b.sessions?.latest_updated_at || 0) - (a.sessions?.latest_updated_at || 0));
}
export function runnerInventoryComplete(runner: RunnerOverview | null | undefined): boolean {
  return Boolean(runner?.connected && runner.projects_available && !runner.projects_truncated
    && runner.visible_project_count === runner.projects.length);
}
export function sessionTitle(value: string): string {
  const title = value.trim().split(/\r?\n/).find(Boolean) || "Workflow Session";
  return title.length > 110 ? title.slice(0, 109) + "…" : title;
}
export { projectPresentationName as projectName, displayProjectPath } from "../../../../../frontend/src/ui/projectPresentation";
type WorkspaceErrorReason = "loadError" | "authenticationRequired" | "permissionDenied" | "serverUnreachable";
function errorReason(result: PromiseSettledResult<unknown>): WorkspaceErrorReason | null {
  if (result.status === "fulfilled") return null;
  // Only accept the closed native error vocabulary; never render an arbitrary
  // error body that could contain credentials or untrusted Server text.
  switch (result.reason?.code) {
    case "workspace_authentication_required": return "authenticationRequired";
    case "workspace_permission_denied": return "permissionDenied";
    case "workspace_server_unreachable": return "serverUnreachable";
    default: return "loadError";
  }
}
function authorizationFailed(reason: WorkspaceErrorReason | null | undefined): boolean {
  return reason === "authenticationRequired" || reason === "permissionDenied";
}
interface WorkspaceValue {
  contextKey: string;
  runners: ServerRunnerSummary[]; fleetStale: boolean; fleetObservedAt: number | null;
  state: DesktopState; runner: RunnerOverview | null; projects: WorkspaceProject[]; windows: WindowSummary[];
  sessions: WorkflowSession[]; loading: boolean; busy: boolean; error: boolean; errorReason: WorkspaceErrorReason; windowsError: boolean; windowsErrorReason: WorkspaceErrorReason; refresh: () => void; removeProject: (id: string) => void; revision: number;
  selection: { kind: "session"; project: string; id: string } | { kind: "window"; id: string } | null;
  setSelection: (value: WorkspaceValue["selection"]) => void;
}
const WorkspaceContext = createContext<WorkspaceValue | null>(null);
export function WorkspaceProvider({ state, suspended = false, preservePollDeadline = false, children }: { state: DesktopState; suspended?: boolean; preservePollDeadline?: boolean; children: ReactNode }) {
  const key = JSON.stringify([state.topology?.server, state.workspace_runner, state.persistent_environment]);
  const [snapshot, setSnapshot] = useState<{ key: string; runners: ServerRunnerSummary[]; fleetObservedAt: number | null; runner: RunnerOverview | null; windows: WindowSummary[]; error: boolean; errorReason: WorkspaceErrorReason | null; windowsError: boolean; windowsErrorReason: WorkspaceErrorReason | null } | null>(null);
  const [loading, setLoading] = useState(false);
  const [revision, setRevision] = useState(0);
  const [selection, setSelection] = useState<WorkspaceValue["selection"]>(null);
  const [removed, setRemoved] = useState<string[]>([]);
  const nextPollAt = useRef<number | null>(null);
  const preservePollDeadlineOnResume = useRef(false);
  const currentKeyRef = useRef(key);
  const pollRunIdRef = useRef(0);
  const activePollRef = useRef<{ id: number; key: string; revision: number; readinessEpoch: number } | null>(null);
  const readinessEpochRef = useRef(0);
  const revisionRef = useRef(revision);
  const mountedRef = useRef(true);
  const busyRef = useRef(false);
  const operationBusyRef = useRef(false);
  const readyRef = useRef(false);
  const timerRef = useRef<number | undefined>(undefined);
  useEffect(() => {
    mountedRef.current = true;
    return () => { mountedRef.current = false; };
  }, []);
  const removeProject = useCallback((id: string) => {
    setRemoved(ids => [...ids, id]);
    setSelection(current => current?.kind === "session" && current.project === id ? null : current);
  }, []);
  const refresh = useCallback(() => {
    nextPollAt.current = null;
    if (busyRef.current) setLoading(true);
    setRevision(value => value + 1);
  }, []);
  // Local refresh starts before the next native operation snapshot arrives.
  const operationBusy = Boolean(state.current_operation);
  const busy = suspended || operationBusy;
  const ready = state.readiness.server === "ready";
  useLayoutEffect(() => {
    const wasReady = readyRef.current;
    const wasOperationBusy = operationBusyRef.current;
    currentKeyRef.current = key;
    revisionRef.current = revision;
    busyRef.current = busy;
    readyRef.current = ready;
    if (wasReady && !ready) {
      readinessEpochRef.current += 1;
      pollRunIdRef.current += 1;
      setLoading(false);
    }
    if (!wasOperationBusy && operationBusy) {
      pollRunIdRef.current += 1;
      activePollRef.current = null;
      nextPollAt.current = null;
      preservePollDeadlineOnResume.current = false;
      setLoading(false);
    }
    operationBusyRef.current = operationBusy;
  }, [key, revision, busy, operationBusy, ready]);
  useEffect(() => {
    // Service operations and Server disconnects invalidate capacity until a new
    // fleet observation, including the gap before recovery polling completes.
    if (operationBusy || !ready) setSnapshot(current => current && ({ ...current, fleetObservedAt: null }));
  }, [operationBusy, ready]);
  useLayoutEffect(() => {
    pollRunIdRef.current += 1;
    activePollRef.current = null;
    nextPollAt.current = null;
    preservePollDeadlineOnResume.current = false;
    setLoading(false);
  }, [key]);
  useEffect(() => {
    const clearPollTimer = () => {
      if (timerRef.current !== undefined) {
        window.clearTimeout(timerRef.current);
        timerRef.current = undefined;
      }
    };
    if (!ready) {
      preservePollDeadlineOnResume.current = false;
      return clearPollTimer;
    }
    if (busy) {
      // Passive Runtime observations are frequent. Preserve the existing
      // Workspace polling deadline across those pauses instead of turning the
      // 15-second Workspace cadence into the Runtime observation cadence.
      preservePollDeadlineOnResume.current = preservePollDeadline && suspended && !operationBusy;
      return clearPollTimer;
    }
    const effectRevision = revision;
    const effectReadinessEpoch = readinessEpochRef.current;
    const pollLoadingApplies = (id: number, pollRevision: number, pollKey: string, readinessEpoch: number) =>
      pollRunIdRef.current === id && mountedRef.current && revisionRef.current === pollRevision
      && currentKeyRef.current === pollKey && readinessEpochRef.current === readinessEpoch;
    const schedulePoll = (delay: number) => {
      clearPollTimer();
      timerRef.current = window.setTimeout(() => {
        timerRef.current = undefined;
        if (!mountedRef.current || busyRef.current || currentKeyRef.current !== key || !readyRef.current) return;
        if (revisionRef.current !== effectRevision || readinessEpochRef.current !== effectReadinessEpoch) return;
        if (document.visibilityState === "visible") void poll();
      }, delay);
    };
    const poll = async () => {
      const pollKey = key;
      if (!mountedRef.current || busyRef.current || currentKeyRef.current !== pollKey || !readyRef.current) return;
      if (revisionRef.current !== effectRevision || readinessEpochRef.current !== effectReadinessEpoch) return;
      const pollRevision = revision;
      const readinessEpoch = readinessEpochRef.current;
      const active = activePollRef.current;
      if (active
        && active.key === pollKey
        && active.revision === pollRevision
        && active.readinessEpoch === readinessEpoch) return;
      const id = ++pollRunIdRef.current;
      activePollRef.current = { id, key: pollKey, revision: pollRevision, readinessEpoch };
      setLoading(true);
      let fleetObservedAt: number | null = null;
      try {
        const [overview, projectInventory, windows] = await Promise.allSettled([
          workspaceQuery<ServerOverview>({ kind: "overview" }).then(value => {
            fleetObservedAt = Date.now();
            return value;
          }),
          workspaceQuery<ServerProjects>({ kind: "projects" }),
          workspaceQuery<{ windows: WindowSummary[] }>({ kind: "windows" }),
        ]);
        if (activePollRef.current?.id !== id) return;
        activePollRef.current = null;
        if (!mountedRef.current || currentKeyRef.current !== pollKey || !readyRef.current
          || readinessEpochRef.current !== readinessEpoch || revisionRef.current !== pollRevision) return;
        const overviewFailed = overview.status === "rejected";
        const projectInventoryFailed = projectInventory.status === "rejected";
        const windowsFailed = windows.status === "rejected";
        const errors = [errorReason(overview), errorReason(projectInventory)];
        const failure = errors.find(authorizationFailed) || errors.find(Boolean) || null;
        const denied = authorizationFailed(failure);
        const windowsFailure = errorReason(windows);
        const runner: RunnerOverview | null = overview.status === "fulfilled"
          ? {
              client_id: state.workspace_runner?.client_id || "",
              connected: true,
              projects_available: overview.value.projects_available,
              visible_project_count: projectInventory.status === "fulfilled"
                ? projectInventory.value.total : overview.value.visible_projects,
              projects: projectInventory.status === "fulfilled"
                ? projectInventory.value.projects : overview.value.projects,
              projects_truncated: projectInventory.status === "fulfilled"
                ? projectInventory.value.truncated : overview.value.projects_truncated,
              recent_sessions: overview.value.recent_sessions,
            }
          : null;
        setSnapshot(old => ({
          key: pollKey,
          fleetObservedAt: denied || operationBusyRef.current ? null : overview.status === "fulfilled" ? fleetObservedAt : old?.key === pollKey ? old.fleetObservedAt : null,
          runners: denied ? [] : overview.status === "fulfilled" ? overview.value.runners || [] : old?.key === pollKey ? old.runners : [],
          runner: denied ? null : runner || (old?.key === pollKey ? old.runner : null),
          windows: denied || authorizationFailed(windowsFailure) ? [] : windows.status === "fulfilled" ? windows.value.windows : old?.key === pollKey ? old.windows : [],
          error: overviewFailed || projectInventoryFailed,
          errorReason: failure,
          windowsError: windowsFailed,
          windowsErrorReason: windowsFailure,
        }));
        if (runner && runnerInventoryComplete(runner)) {
          setRemoved(ids => ids.filter(id => runner.projects.some(project => project.id === id)));
        }
        nextPollAt.current = Date.now() + 15_000;
        schedulePoll(15_000);
      } finally {
        if (pollLoadingApplies(id, pollRevision, pollKey, readinessEpoch)) setLoading(false);
      }
    };
    const visible = () => { if (document.visibilityState === "visible") refresh(); };
    document.addEventListener("visibilitychange", visible);
    const preserveDeadline = preservePollDeadlineOnResume.current;
    preservePollDeadlineOnResume.current = false;
    const remaining = preserveDeadline && nextPollAt.current !== null
      ? nextPollAt.current - Date.now()
      : 0;
    if (remaining > 0) schedulePoll(remaining);
    else void poll();
    return () => {
      clearPollTimer();
      document.removeEventListener("visibilitychange", visible);
    };
  }, [key, ready, busy, suspended, preservePollDeadline, operationBusy, revision, refresh]);
  useEffect(() => { setSelection(null); setRemoved([]); }, [key]);
  const current = snapshot?.key === key ? snapshot : null;
  const projects = useMemo(() => authorizationFailed(current?.errorReason) ? [] : mergeProjects(current?.runner?.projects || [],
    state.saved_projects || (state.project ? [state.project] : []), runnerInventoryComplete(current?.runner), state.workspace_runner?.client_id)
    .filter(project => !removed.includes(project.id)),
  [current?.runner, current?.errorReason, state.saved_projects, state.project, state.workspace_runner?.client_id, removed]);
  useEffect(() => {
    setSelection(current => current?.kind === "session" && !projects.some(project => project.id === current.project) ? null : current);
  }, [projects]);
  const ids = new Set(projects.map(project => project.id));
  return <WorkspaceContext.Provider value={{ contextKey: key, state, runners: current?.runners || [], fleetObservedAt: current?.fleetObservedAt ?? null, fleetStale: !ready || Boolean(current?.error), runner: current?.runner || null, projects,
    windows: (current?.windows || []).filter(row => !row.last_project || ids.has(row.last_project)),
    sessions: (current?.runner?.recent_sessions?.sessions || []).filter(session => !session.project_id || ids.has(session.project_id)), loading: ready && !busy && (loading || !current),
    busy, error: Boolean(current?.error), errorReason: current?.errorReason || "loadError", windowsError: Boolean(current?.windowsError), windowsErrorReason: current?.windowsErrorReason || "loadError", refresh, removeProject, revision, selection, setSelection,
  }}>{children}</WorkspaceContext.Provider>;
}
export function useWorkspace(): WorkspaceValue {
  const context = useContext(WorkspaceContext);
  if (!context) throw new Error("WorkspaceProvider is required");
  return context;
}
