<script lang="ts">
  import { Grid, Row, Column, Loading } from 'carbon-components-svelte';
  import { loading, error, data } from './lib/stores/summary';
  import ProductTile from './lib/components/ProductTile.svelte';
  import SummaryHeader from './lib/components/SummaryHeader.svelte';
</script>

{#if $loading}
  <Loading active description="Loading catalogue..." />
{:else if $error}
  <Grid>
    <Row>
      <Column sm={4} md={8} lg={16}>
        <h1>Companies Catalogue</h1>
        <p style="color: #da1e28;">{$error}</p>
      </Column>
    </Row>
  </Grid>
{:else if $data}
  <Grid fullWidth>
    <Row>
      <Column sm={4} md={8} lg={16}>
        <SummaryHeader summary={{
          generated_at: $data.generated_at,
          total_size_bytes: $data.total_size_bytes,
          total_avg_size_last5: $data.total_avg_size_last5,
        }} />
      </Column>
    </Row>

    <Row style="gap: 1rem;">
      {#each $data.products.slice().sort((a, b) => a.product.localeCompare(b.product)) as p}
        <Column sm={4} md={4} lg={4}>
          <ProductTile product={p} />
        </Column>
      {/each}
    </Row>
  </Grid>
{:else}
  <Grid>
    <Row>
      <Column sm={4} md={8} lg={16}>
        <h1>Companies Catalogue</h1>
        <p>No data found.</p>
      </Column>
    </Row>
  </Grid>
{/if}

<style>
  :global(body) { margin: 0; }
</style>
