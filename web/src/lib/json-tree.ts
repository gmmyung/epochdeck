export const JSON_TREE_PAGE_SIZE = 100;
export const JSON_TREE_SEARCH_MAX_LENGTH = 256;

export type JsonTreeSearchChild = {
  name: string;
  value: unknown;
  match: JsonTreeSearchMatch;
};

export type JsonTreeSearchMatch = {
  keyMatches: boolean;
  valueMatches: boolean;
  matchCount: number;
  children: JsonTreeSearchChild[];
};

export function nodeChildCount(value: unknown): number {
  if (Array.isArray(value)) return value.length;
  if (typeof value === "object" && value !== null) return Object.keys(value).length;
  return 0;
}

export function visibleChildEntries(
  value: Record<string, unknown> | unknown[],
  limit: number,
  offset = 0,
): Array<[string, unknown]> {
  const boundedLimit = Math.max(0, Math.floor(limit));
  const boundedOffset = Math.max(0, Math.floor(offset));
  if (Array.isArray(value)) {
    return value
      .slice(boundedOffset, boundedOffset + boundedLimit)
      .map((child, index) => [String(boundedOffset + index), child]);
  }
  return Object.keys(value)
    .slice(boundedOffset, boundedOffset + boundedLimit)
    .map((key) => [key, value[key]]);
}

export function normalizeJsonTreeSearch(query: string): string {
  return query.trim().slice(0, JSON_TREE_SEARCH_MAX_LENGTH).toLowerCase();
}

export function jsonTreeScalarText(value: unknown): string {
  if (value === null) return "null";
  return String(value);
}

export function searchJsonTree(
  value: unknown,
  query: string,
  name = "",
): JsonTreeSearchMatch | null {
  const normalizedQuery = normalizeJsonTreeSearch(query);
  if (!normalizedQuery) return null;
  return searchJsonTreeNode(value, normalizedQuery, name);
}

function searchJsonTreeNode(
  value: unknown,
  normalizedQuery: string,
  name: string,
): JsonTreeSearchMatch | null {
  const keyMatches = name.length > 0 && name.toLowerCase().includes(normalizedQuery);
  if (typeof value !== "object" || value === null) {
    const valueMatches = scalarSearchText(value).toLowerCase().includes(normalizedQuery);
    if (!keyMatches && !valueMatches) return null;
    return {
      keyMatches,
      valueMatches,
      matchCount: 1,
      children: [],
    };
  }

  const children: JsonTreeSearchChild[] = [];
  for (const [childName, childValue] of visibleChildEntries(
    value as Record<string, unknown> | unknown[],
    nodeChildCount(value),
  )) {
    const match = searchJsonTreeNode(childValue, normalizedQuery, childName);
    if (match) children.push({ name: childName, value: childValue, match });
  }
  if (!keyMatches && children.length === 0) return null;
  return {
    keyMatches,
    valueMatches: false,
    matchCount:
      (keyMatches ? 1 : 0) + children.reduce((count, child) => count + child.match.matchCount, 0),
    children,
  };
}

function scalarSearchText(value: unknown): string {
  return jsonTreeScalarText(value);
}
