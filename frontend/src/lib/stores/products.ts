import { writable, get } from 'svelte/store';

// Simple key->JSON cache for product data
export const productsCache = writable<Record<string, any>>({});

export async function getProduct(id: string): Promise<any | null> {
  const cache = get(productsCache);
  if (cache[id]) return cache[id];

  const url = new URL(`${encodeURIComponent(id)}.json`, import.meta.env.VITE_S3_URL)
  const res = await fetch(url);
  if (!res.ok) {
    return null; // treat any non-OK as not found/failure
  }
  const json = await res.json();
  productsCache.update((c) => ({ ...c, [id]: json }));
  return json;
}
