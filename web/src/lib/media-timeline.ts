import type { RichValueSummary } from "./api";

export type MediaGroup = {
  id: string;
  key: string;
  kind: RichValueSummary["kind"];
  values: RichValueSummary[];
};

export function groupRichValues(values: RichValueSummary[]): MediaGroup[] {
  const grouped = new Map<string, MediaGroup>();
  for (const value of values) {
    const id = mediaGroupId(value.kind, value.key);
    const group = grouped.get(id);
    if (group) group.values.push(value);
    else grouped.set(id, { id, key: value.key, kind: value.kind, values: [value] });
  }

  const groups = [...grouped.values()];
  for (const group of groups) group.values.sort(compareRichValues);
  groups.sort(
    (left, right) => left.key.localeCompare(right.key) || left.kind.localeCompare(right.kind),
  );
  return groups;
}

function mediaGroupId(kind: RichValueSummary["kind"], key: string): string {
  return `${kind}\0${key}`;
}

export function selectedMediaIndex(group: MediaGroup, selectedId: string | undefined): number {
  if (selectedId) {
    const selected = group.values.findIndex((value) => value.id === selectedId);
    if (selected >= 0) return selected;
  }
  return Math.max(group.values.length - 1, 0);
}

function compareRichValues(left: RichValueSummary, right: RichValueSummary): number {
  return (
    left.step - right.step ||
    left.timestamp_ms - right.timestamp_ms ||
    left.id.localeCompare(right.id)
  );
}
