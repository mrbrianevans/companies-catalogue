import { writable } from 'svelte/store';
import type { MetadataSummary } from '../types';

export const loading = writable(true);
export const error = writable<string | null>(null);
export const data = writable<MetadataSummary | null>(null);

const SUMMARY_URL = import.meta.env.DEV
  ? '/sftp_file_metadata_summary.json'
  : new URL('/sftp_file_metadata_summary.json', import.meta.env.VITE_S3_URL);

export async function fetchSummary() {
  loading.set(true);
  error.set(null);
  try {
    const res = await fetch(SUMMARY_URL, {});
    if (!res.ok) throw new Error(`Failed to load summary: ${res.status} ${res.statusText}`);
    const json = (await res.json()) as MetadataSummary;
    data.set(json);
  } catch (e: any) {
    error.set(e?.message ?? 'Unknown error fetching summary');
  } finally {
    loading.set(false);
  }
}

// Auto-load on module import so App does not need to trigger fetching
fetchSummary().catch(() => {
  // error store already set in fetchSummary
});
