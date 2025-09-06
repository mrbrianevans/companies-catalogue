<script lang="ts">
    import {
        Grid,
        Row,
        Column,
        Tile,
        CodeSnippet,
        Tag,
        InlineLoading,
        StructuredList,
        StructuredListBody, StructuredListRow, StructuredListCell, Button
    } from 'carbon-components-svelte';
  import type { ProductSummary } from '../lib/types';
  import { getProduct } from '../lib/stores/products';
  import {fmtBytes, fmtNumber} from "../lib/utils";

  export let productId: string;
  export let summary: ProductSummary;

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
  
  const productDetailsPromise = productId?getProduct(productId):undefined;
</script>

<Grid fullWidth padding>
  <Row>
    <Column sm={4} md={8} lg={16}>
      <h1 style="margin: 1rem 0;">{productDisplayName(productId)} <Tag type="cool-gray" style="margin-left: 0.5rem;">{productId}</Tag></h1>
      <p style="margin: 0 0 1rem; color: #6f6f6f;">
        Additional details are fetched from S3 if available.
        <Tag type="cool-gray" style="margin-left: 0.5rem;">{productId}.json</Tag>
      </p>
    </Column>
  </Row>

  <Row>
    <Column sm={4} md={8} lg={16}>
      <Tile>
        <h3 style="margin-top: 0;">Summary</h3>
        <div style="display: grid; grid-template-columns: 1fr auto; row-gap: 0.25rem; column-gap: 1rem; font-size: 0.95rem;max-width:30rem">
          <div>Latest date</div>
          <div><strong>{summary.latest_date}</strong></div>

          <div>Latest files</div>
          <div><strong>{summary.latest_files.length}</strong></div>

            <div>Avg days between runs</div>
            <div>{fmtNumber(summary.avg_interval_days)}</div>

            <div>Avg size of run</div>
            <div>{summary.avg_size_last5 == null ? '—' : fmtBytes(summary.avg_size_last5)}</div>

          <div>Most recent modified</div>
          <div>{new Date(summary.latest_last_modified).toLocaleString()}</div>
        </div>
      </Tile>
    </Column>
  </Row>

    <Row>
        <Column sm={4} md={6} lg={8}>
            <Tile>
                <h3 style="margin-top: 0;">Docs</h3>
                {#if summary.docs.length > 0}
                    <StructuredList condensed flush>
                        <StructuredListBody>
                        {#each summary.docs as doc}
                            <StructuredListRow>
                                <StructuredListCell>{doc}</StructuredListCell>
                                <StructuredListCell><Button href={new URL(doc, import.meta.env.VITE_S3_URL).href}>Download</Button></StructuredListCell>
                            </StructuredListRow>
                            {/each}
                        </StructuredListBody>
                    </StructuredList>
                    {:else}
                    <p style="color: #6f6f6f;">No docs found.</p>
                    {/if}
            </Tile>
        </Column>
    </Row>

  <Row>
    <Column sm={4} md={8} lg={16}>
      <Tile>
        <h3 style="margin-top: 0;">Additional detail (from S3)</h3>

          {#await productDetailsPromise}
              <InlineLoading description="Loading extra details..."/>
          {:then details}
              {#if details}
                  <CodeSnippet type="multi" feedback="Copied!" >
                      {JSON.stringify(details, null, 2)}
                  </CodeSnippet>
              {:else}
                  <p style="color: #6f6f6f;">No additional detail found.</p>
              {/if}
          {:catch error}
              <p style="color: #da1e28;">Error loading details. {error.message}</p>
          {/await}
        <p style="margin-top: 1rem;"><a href="/#/">Back to home</a></p>
      </Tile>
    </Column>
  </Row>
</Grid>
