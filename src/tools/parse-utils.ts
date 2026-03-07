const WINDOWS_RESERVED_FILE_NAMES = /^(con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])$/i;

export function sanitizeFileName(name: string, fallback: string): string {
  const sanitized = name
    .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "_")
    .replace(/\.{2,}/g, ".")
    .replace(/[. ]+$/g, "")
    .trim();
  if (!sanitized) return fallback;
  const dotIndex = sanitized.indexOf(".");
  const baseName = dotIndex === -1 ? sanitized : sanitized.slice(0, dotIndex);
  const extension = dotIndex === -1 ? "" : sanitized.slice(dotIndex);
  if (WINDOWS_RESERVED_FILE_NAMES.test(baseName)) return `${baseName}_${extension}`;
  return sanitized;
}

export function asString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

export function asRecord(value: unknown): Record<string, unknown> | undefined {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return undefined;
}
