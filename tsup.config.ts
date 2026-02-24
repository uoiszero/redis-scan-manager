import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['cjs', 'esm'],
  dts: true,
  clean: true,
  minify: false,
  sourcemap: true,
  splitting: false,
  treeshake: true,
  external: ['ioredis'],
  outDir: 'dist',
  onSuccess: async () => {
    console.log('Build successfully completed!');
  },
});
