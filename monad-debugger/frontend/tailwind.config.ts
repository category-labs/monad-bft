import type { Config } from 'tailwindcss';
import { scopedPreflightStyles, isolateInsideOfContainer } from 'tailwindcss-scoped-preflight';

const config: Config = {
  content: [
    './index.html',
    './src/**/*.{js,ts,jsx,tsx,css,md,mdx,html,json,scss}',
  ],
  darkMode: 'class',
  theme: {
    extend: {},
  },
  plugins: [
    scopedPreflightStyles({
      isolationStrategy: isolateInsideOfContainer('#root'),
    }),
  ],
};

export default config;
