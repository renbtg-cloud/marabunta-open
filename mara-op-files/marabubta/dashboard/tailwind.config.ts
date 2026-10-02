// Marabunta - Licensed under the MIT License.
import type { Config } from 'tailwindcss';

export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        marabunta: {
          bg: '#0a0e17',
          bg2: '#111827',
          bg3: '#1a2234',
          bgc: '#161f31',
          bgd: '#0d1321',
          t1: '#e8ecf4',
          t2: '#94a3b8',
          muted: '#64748b',
          cyan: '#22d3ee',
          green: '#34d399',
          amber: '#fbbf24',
          rose: '#fb7185',
          violet: '#a78bfa',
          blue: '#60a5fa',
          border: '#1e293b',
        },
      },
      fontFamily: {
        mono: ['IBM Plex Mono', 'monospace'],
        serif: ['Instrument Serif', 'serif'],
        sans: ['DM Sans', 'sans-serif'],
      },
    },
  },
  plugins: [],
} satisfies Config;
