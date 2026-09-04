export type MetricColumnCount = "auto" | "1" | "2" | "3" | "4";

const STORAGE_KEY = "epochdeck:metric-columns";
const COLUMN_COUNTS = new Set<MetricColumnCount>(["auto", "1", "2", "3", "4"]);
type PreferenceStorage = Pick<Storage, "getItem" | "setItem">;

export function readMetricColumnCount(
  storage: PreferenceStorage | null = browserStorage(),
): MetricColumnCount {
  const value = storage?.getItem(STORAGE_KEY);
  return value && COLUMN_COUNTS.has(value as MetricColumnCount)
    ? (value as MetricColumnCount)
    : "auto";
}

export function rememberMetricColumnCount(
  value: MetricColumnCount,
  storage: PreferenceStorage | null = browserStorage(),
): MetricColumnCount {
  if (!COLUMN_COUNTS.has(value)) return "auto";
  storage?.setItem(STORAGE_KEY, value);
  return value;
}

function browserStorage(): PreferenceStorage | null {
  return typeof window === "undefined" ? null : window.localStorage;
}
