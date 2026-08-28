export type Health = {
  service: string;
  version: string;
  status: "healthy" | "unhealthy";
};

export async function getHealth(signal?: AbortSignal): Promise<Health> {
  const response = await fetch("/api/v1/health", { signal });
  if (!response.ok) {
    throw new Error(`Health request failed with HTTP ${response.status}`);
  }
  return (await response.json()) as Health;
}
