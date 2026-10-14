export function orderByIds<T extends { id: string }>(
  items: T[],
  savedOrder: string[] | null,
  labelKey: keyof T & string,
): T[] {
  const compare = (a: T, b: T) =>
    String(a[labelKey]).localeCompare(String(b[labelKey]));
  if (!savedOrder) {
    return [...items].sort(compare);
  }
  const orderMap = new Map(savedOrder.map((id, i) => [id, i]));
  const known = items.filter((item) => orderMap.has(item.id));
  const unknown = items.filter((item) => !orderMap.has(item.id));
  known.sort((a, b) => orderMap.get(a.id)! - orderMap.get(b.id)!);
  unknown.sort(compare);
  return [...known, ...unknown];
}
