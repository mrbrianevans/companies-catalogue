<script lang="ts">
  import type { MetadataSummary } from '../types';
  import { fmtBytes } from '../utils';

  export let summary: Pick<MetadataSummary, 'generated_at' | 'total_size_bytes' | 'total_avg_size_last5'>;
</script>

<h1 style="margin: 1rem 0;">Companies Catalogue</h1>
<p style="margin: 0 0 1rem; color: #6f6f6f;">
  Generated at: {new Date(summary.generated_at).toLocaleString()} |
  Total size: {fmtBytes(summary.total_size_bytes)} |
  Avg size of all products: {fmtBytes(summary.total_avg_size_last5)}
</p>
