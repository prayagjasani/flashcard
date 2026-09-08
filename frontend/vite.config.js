import { defineConfig } from 'vite';

export default defineConfig({
  base: '/static/react/',
  build: { outDir: '../static/react', emptyOutDir: true },
  server: {
    host: '127.0.0.1',
    proxy: {
      // Requests outside Vite's asset prefix go to the existing Python app.
      '^/(?!static/react/)': 'http://127.0.0.1:8000',
    },
  },
});
