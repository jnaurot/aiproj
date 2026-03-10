import devtoolsJson from 'vite-plugin-devtools-json';
import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { resolve } from 'node:path';

export default defineConfig({
    plugins: [tailwindcss(), sveltekit(), devtoolsJson()],
    build: {
        sourcemap:true,
    },
    test: {
        environment: 'node',
        include: ['src/**/*.test.ts']
    },
    server: {
        fs: {
            allow: [resolve(__dirname, 'shared')]
        }
    }
});
