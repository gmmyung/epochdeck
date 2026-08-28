import type { ArtifactEntry } from "./api";

export type ArtifactBreadcrumb = {
  label: string;
  path: string;
};

export type ArtifactDirectoryItem = {
  kind: "directory";
  name: string;
  path: string;
  fileCount: number;
  size: number;
};

export type ArtifactFileItem = {
  kind: "file";
  name: string;
  path: string;
  entry: ArtifactEntry;
  size: number;
};

export type ArtifactBrowserItem = ArtifactDirectoryItem | ArtifactFileItem;

export function artifactDirectoryItems(
  entries: ArtifactEntry[],
  directory: string,
): ArtifactBrowserItem[] {
  const normalized = normalizeDirectory(directory);
  const prefix = normalized ? `${normalized}/` : "";
  const directories = new Map<string, ArtifactDirectoryItem>();
  const files: ArtifactFileItem[] = [];

  for (const entry of entries) {
    if (!entry.path.startsWith(prefix)) continue;
    const relative = entry.path.slice(prefix.length);
    if (!relative || relative.startsWith("/")) continue;
    const separator = relative.indexOf("/");
    if (separator < 0) {
      files.push({
        kind: "file",
        name: relative,
        path: entry.path,
        entry,
        size: entry.blob.size,
      });
      continue;
    }

    const name = relative.slice(0, separator);
    const path = prefix + name;
    const existing = directories.get(name);
    if (existing) {
      existing.fileCount += 1;
      existing.size += entry.blob.size;
    } else {
      directories.set(name, {
        kind: "directory",
        name,
        path,
        fileCount: 1,
        size: entry.blob.size,
      });
    }
  }

  return [
    ...[...directories.values()].sort((left, right) => left.name.localeCompare(right.name)),
    ...files.sort((left, right) => left.name.localeCompare(right.name)),
  ];
}

export function artifactBreadcrumbs(directory: string): ArtifactBreadcrumb[] {
  const segments = normalizeDirectory(directory).split("/").filter(Boolean);
  const breadcrumbs: ArtifactBreadcrumb[] = [{ label: "Files", path: "" }];
  let path = "";
  for (const segment of segments) {
    path = path ? `${path}/${segment}` : segment;
    breadcrumbs.push({ label: segment, path });
  }
  return breadcrumbs;
}

export function artifactTotalSize(entries: ArtifactEntry[]): number {
  return entries.reduce((total, entry) => total + entry.blob.size, 0);
}

function normalizeDirectory(directory: string): string {
  return directory.split("/").filter(Boolean).join("/");
}
