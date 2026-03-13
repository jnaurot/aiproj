import devtoolsJson from 'vite-plugin-devtools-json';
import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { resolve } from 'node:path';

export default defineConfig({
    plugins: [tailwindcss(), sveltekit(), devtoolsJson()],
    build: {
        sourcemap: true,
    },
    test: {
        environment: 'node',
        include: ['src/**/*.test.ts']
    },
    server: {
        host: true,
        fs: {
            allow: [resolve(__dirname, 'shared')]
        },
        proxy: {
            '/api': {
                target: 'http://127.0.0.1:8000',
                changeOrigin: true,
                rewrite: (path) => path.replace(/^\/api/, '')
            }
        }
    }
});