/** Parse JSON text. Invalid JSON is reported; the value is not schema-validated. */
export function parseJSON<T>(raw: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(`invalid JSON: ${detail}`);
  }
}
