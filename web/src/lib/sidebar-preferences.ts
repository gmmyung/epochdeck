import { runStyle, type RunStyle } from "./comparison-state";

export type RunStylePreferences = Record<string, RunStyle>;

export const DEFAULT_SIDEBAR_WIDTH = 280;
export const MIN_SIDEBAR_WIDTH = 220;
const MAX_SIDEBAR_WIDTH = 640;
const MIN_WORKSPACE_CONTENT_WIDTH = 480;

const MAX_RUN_STYLE_PREFERENCES = 256;
const MAX_RUN_STYLE_STORAGE_BYTES = 64 * 1024;
const RUN_STYLE_STORAGE_KEY = "epochdeck:run-styles";
const SIDEBAR_WIDTH_STORAGE_KEY = "epochdeck:sidebar-width";
const SIDEBAR_COLLAPSED_STORAGE_KEY = "epochdeck:sidebar-collapsed";
const RUN_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const COLOR_PATTERN = /^#[0-9a-f]{6}$/i;
const LINE_PATTERNS = new Set<RunStyle["pattern"]>(["solid", "dash", "dot", "dash-dot"]);

type PreferenceStorage = Pick<Storage, "getItem" | "setItem">;

export function resolveRunStyle(runId: string, preferences: RunStylePreferences): RunStyle {
  const preferred = Object.prototype.hasOwnProperty.call(preferences, runId)
    ? preferences[runId]
    : undefined;
  return preferred ? { ...preferred } : runStyle(runId);
}

export function readRunStylePreferences(
  storage: PreferenceStorage | null = browserStorage(),
): RunStylePreferences {
  if (!storage) return {};
  try {
    const raw = storage.getItem(RUN_STYLE_STORAGE_KEY);
    if (raw === null) return {};
    if (new TextEncoder().encode(raw).byteLength > MAX_RUN_STYLE_STORAGE_BYTES) return {};
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return {};
    return boundedRunStyles(parsed);
  } catch {
    return {};
  }
}

export function rememberRunStylePreference(
  current: RunStylePreferences,
  runId: string,
  style: RunStyle,
  storage: PreferenceStorage | null = browserStorage(),
): RunStylePreferences {
  if (!validRunId(runId) || !validRunStyle(style)) return current;
  const entries: unknown[] = Object.entries(current).filter(
    ([candidateRunId, candidateStyle]) =>
      candidateRunId !== runId && validRunId(candidateRunId) && validRunStyle(candidateStyle),
  );
  entries.push([runId, { ...style }]);
  const next = boundedRunStyles(entries);
  try {
    storage?.setItem(RUN_STYLE_STORAGE_KEY, JSON.stringify(Object.entries(next)));
  } catch {
    // Browser storage is optional; the in-memory preference still applies.
  }
  return next;
}

export function forgetRunStylePreference(
  current: RunStylePreferences,
  runId: string,
  storage: PreferenceStorage | null = browserStorage(),
): RunStylePreferences {
  const next = Object.fromEntries(
    Object.entries(current).filter(
      ([candidateRunId, style]) =>
        candidateRunId !== runId && validRunId(candidateRunId) && validRunStyle(style),
    ),
  );
  try {
    storage?.setItem(RUN_STYLE_STORAGE_KEY, JSON.stringify(Object.entries(next)));
  } catch {
    // Resetting remains effective for the current session without storage.
  }
  return next;
}

export function maximumSidebarWidth(viewportWidth: number): number {
  if (!Number.isFinite(viewportWidth)) return MAX_SIDEBAR_WIDTH;
  return Math.max(
    MIN_SIDEBAR_WIDTH,
    Math.min(MAX_SIDEBAR_WIDTH, viewportWidth - MIN_WORKSPACE_CONTENT_WIDTH),
  );
}

export function clampSidebarWidth(width: number, viewportWidth: number): number {
  const maximum = maximumSidebarWidth(viewportWidth);
  if (!Number.isFinite(width)) return Math.min(DEFAULT_SIDEBAR_WIDTH, maximum);
  return Math.min(Math.max(Math.round(width), MIN_SIDEBAR_WIDTH), maximum);
}

export function readSidebarWidth(
  viewportWidth: number,
  storage: PreferenceStorage | null = browserStorage(),
): number {
  if (!storage) return clampSidebarWidth(DEFAULT_SIDEBAR_WIDTH, viewportWidth);
  try {
    const raw = storage.getItem(SIDEBAR_WIDTH_STORAGE_KEY);
    return raw === null
      ? clampSidebarWidth(DEFAULT_SIDEBAR_WIDTH, viewportWidth)
      : clampSidebarWidth(Number(raw), viewportWidth);
  } catch {
    return clampSidebarWidth(DEFAULT_SIDEBAR_WIDTH, viewportWidth);
  }
}

export function rememberSidebarWidth(
  width: number,
  viewportWidth: number,
  storage: PreferenceStorage | null = browserStorage(),
): number {
  const next = clampSidebarWidth(width, viewportWidth);
  try {
    storage?.setItem(SIDEBAR_WIDTH_STORAGE_KEY, String(next));
  } catch {
    // Resizing remains usable when browser storage is unavailable.
  }
  return next;
}

export function readSidebarCollapsed(
  storage: PreferenceStorage | null = browserStorage(),
): boolean {
  if (!storage) return false;
  try {
    return storage.getItem(SIDEBAR_COLLAPSED_STORAGE_KEY) === "true";
  } catch {
    return false;
  }
}

export function rememberSidebarCollapsed(
  collapsed: boolean,
  storage: PreferenceStorage | null = browserStorage(),
): boolean {
  try {
    storage?.setItem(SIDEBAR_COLLAPSED_STORAGE_KEY, String(collapsed));
  } catch {
    // Collapsing remains usable for the current session without storage.
  }
  return collapsed;
}

function boundedRunStyles(entries: unknown[]): RunStylePreferences {
  const retained = new Map<string, RunStyle>();
  for (const entry of entries) {
    if (!Array.isArray(entry) || entry.length !== 2) continue;
    const [runId, style] = entry;
    if (!validRunId(runId) || !validRunStyle(style)) continue;
    retained.delete(runId);
    retained.set(runId, { ...style });
    while (retained.size > MAX_RUN_STYLE_PREFERENCES) {
      const oldest = retained.keys().next().value;
      if (oldest === undefined) break;
      retained.delete(oldest);
    }
  }
  return Object.fromEntries(retained);
}

function validRunId(value: unknown): value is string {
  return typeof value === "string" && RUN_ID_PATTERN.test(value);
}

function validRunStyle(value: unknown): value is RunStyle {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const candidate = value as Partial<RunStyle>;
  return (
    typeof candidate.color === "string" &&
    COLOR_PATTERN.test(candidate.color) &&
    candidate.pattern !== undefined &&
    LINE_PATTERNS.has(candidate.pattern)
  );
}

function browserStorage(): PreferenceStorage | null {
  try {
    return typeof window === "undefined" ? null : window.localStorage;
  } catch {
    return null;
  }
}
