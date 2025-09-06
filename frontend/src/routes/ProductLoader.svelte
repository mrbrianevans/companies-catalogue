<script lang="ts">
  import { Loading } from 'carbon-components-svelte';
  import ProductPage from './ProductPage.svelte';
  import NotFound from './NotFound.svelte';
  import { loading as summaryLoading, data as summaryData } from '../lib/stores/summary';

  export let params: { prod_id: string };

  const summaryProduct = $summaryData?.products?.find((p) => p.product === params.prod_id)

</script>

{#if $summaryLoading}
  <Loading active description="Loading product..." />
{:else if !summaryProduct}
  <NotFound />
{:else}
  <ProductPage productId={params.prod_id} summary={summaryProduct} />
{/if}
