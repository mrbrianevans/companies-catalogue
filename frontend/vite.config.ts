import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

// https://vite.dev/config/
export default defineConfig({
  plugins: [svelte()],
  server: {
    proxy: {
      // Proxy the public metadata file during development so the app can fetch from localhost
      '/sftp_file_metadata_summary.json': {
        target: 'https://pub-022c8bf97425454a9b539021ace956f8.r2.dev',
        changeOrigin: true,
        secure: true,
      },
    },
  },
})
