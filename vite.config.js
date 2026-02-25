import devtoolsJson from 'vite-plugin-devtools-json';
import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { resolve } from 'node:path';

export default defineConfig({
    plugins: [tailwindcss(), sveltekit(), devtoolsJson()],
    server: {
        fs: {
            allow: [resolve(__dirname, 'shared')]
        },
        proxy: {
            "/runs": {
                target: "http://localhost:8000", // your FastAPI port
                changeOrigin: true
            },
        }
    }
});
