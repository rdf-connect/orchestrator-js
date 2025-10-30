import { defineConfig } from 'vitest/config'

export default defineConfig({
    test: {
        coverage: {
            enabled: true,
            include: ['lib/'],
            exclude: ['**/*.d.ts', 'lib/generated', '**/*.tsbuildinfo'], // Exclude all declaration files
        },
    },
})
