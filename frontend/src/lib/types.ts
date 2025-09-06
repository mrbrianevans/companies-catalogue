// Shared types for the frontend app
export interface ProductSummary {
  product: string;
  latest_files: string[];
  latest_date: string;
  latest_last_modified: string;
  avg_interval_days: number | null;
  avg_size_last5: number | null;
  last5_dates: string[];
  docs: string[];
}

export interface MetadataSummary {
  generated_at: string;
  most_recent_last_modified: string | null;
  total_avg_size_last5: number;
  total_size_bytes: number;
  products: ProductSummary[];
}
