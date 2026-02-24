import esbuild from 'esbuild';

async function build() {
  // Build CJS
  await esbuild.build({
    entryPoints: ['redis_scan_manager.js'],
    bundle: true,
    platform: 'node',
    target: ['node14'],
    outfile: 'dist/index.cjs',
    format: 'cjs',
    external: ['ioredis', 'crypto'],
  });

  // Build ESM
  await esbuild.build({
    entryPoints: ['redis_scan_manager.js'],
    bundle: true,
    platform: 'node',
    target: ['node14'],
    outfile: 'dist/index.mjs',
    format: 'esm',
    external: ['ioredis', 'crypto'],
  });

  console.log('Build complete');
}

build().catch(() => process.exit(1));
