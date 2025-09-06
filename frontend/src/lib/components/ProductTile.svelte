<script lang="ts">
  import { Tile, Tag, Accordion, AccordionItem, CodeSnippet } from 'carbon-components-svelte';
  import type { ProductSummary } from '../types';
  import { fmtBytes, fmtNumber } from '../utils';

  export let product: ProductSummary;

  function productDisplayName(code: string) {
    const m = code.match(/^([a-zA-Z]+)(\d+)$/);
    if (!m) return code;
    let name = m[1];
    const num = m[2];
    if (name.toLowerCase() === 'prod') {
      name = 'Product';
    } else {
      name = name.charAt(0).toUpperCase() + name.slice(1).toLowerCase();
    }
    return `${name} ${num}`;
  }

  $: displayName = productDisplayName(product.product);
</script>

<Tile>
  <div style="display:flex; align-items:baseline; justify-content:space-between; gap:0.5rem;">
    <h3 style="margin: 0;"><a href="/#/{product.product}" style="text-decoration: none; color: inherit;">{displayName}</a></h3>
    <Tag type="cool-gray">{product.product}</Tag>
  </div>

  <div style="font-size: 0.875rem; color: var(--cds-text-secondary, #525252); margin-top: 0.25rem;">
    Latest date: {product.latest_date}
  </div>

  <div style="display: grid; grid-template-columns: 1fr auto; row-gap: 0.25rem; column-gap: 1rem; font-size: 0.95rem; margin-top: 0.5rem;">
    <div>Latest files</div>
    <div><strong>{product.latest_files.length}</strong></div>

    <div>Avg interval (days)</div>
    <div>{fmtNumber(product.avg_interval_days)}</div>

    <div>Avg size (last 5)</div>
    <div>{product.avg_size_last5 == null ? '—' : fmtBytes(product.avg_size_last5)}</div>

    <div>Most recent modified</div>
    <div>{new Date(product.latest_last_modified).toLocaleString()}</div>
  </div>

  <Accordion style="margin-top: 0.75rem;padding-right:0" >
    <AccordionItem title="Latest paths">
      <CodeSnippet type="multi" feedback="Copied!">
{product.latest_files.join('\n')}
      </CodeSnippet>
    </AccordionItem>
  </Accordion>
</Tile>


<style>
    :global(.bx--accordion__content){
        padding-right:0 !important;
    }
</style>