<script lang="ts">
  import Router from 'svelte-spa-router';
  import { wrap } from 'svelte-spa-router/wrap';
  import Home from './routes/Home.svelte';
  import NotFound from './routes/NotFound.svelte';

  const routes = {
    '/': Home,
    '/:prod_id': wrap({
      asyncComponent: () => import('./routes/ProductLoader.svelte'),
    }),
    '*': NotFound,
  };
</script>

<Router {routes} />

<style>
  :global(body) { margin: 0; }
</style>
