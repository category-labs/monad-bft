import { defineConfig } from 'vite';
import solidPlugin from 'vite-plugin-solid';
// import devtools from 'solid-devtools/vite';
import wasm from 'vite-plugin-wasm';

export default defineConfig({
  plugins: [
    /* 
    Uncomment the following line to enable solid-devtools.
    For more info see https://github.com/thetarnav/solid-devtools/tree/main/packages/extension#readme
    */
    // devtools(),
    solidPlugin(),
    wasm(),
  ],
  server: {
    port: 3000,
  },
  build: {
    target: 'esnext',
    rollupOptions: {
      output: {
        inlineDynamicImports: true,
        // // Single file output
        // entryFileNames: '[name].js',
        // chunkFileNames: '[name].js',
        // assetFileNames: '[name][extname]'
      }
    },
    // Inline CSS and other assets
    cssCodeSplit: false,
    assetsInlineLimit: 100000000, // Inline everything
  },
});
