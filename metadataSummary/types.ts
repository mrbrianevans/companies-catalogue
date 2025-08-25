// Shared TypeScript types for metadata summary outputs

export interface ProductSummary {
  product: string;
  latest_files: string[];
  latest_date: string; // ISO date (YYYY-MM-DD) for the latest files
  latest_last_modified: string; // ISO timestamp: most recent last_modified among latest files
  avg_interval_days: number | null;
  avg_size_last5: number | null;
  last5_dates: string[];
}

export interface MetadataSummary {
  generated_at: string;
  most_recent_last_modified: string | null;
  total_avg_size_last5: number;
  total_size_bytes: number;
  products: ProductSummary[];
}
