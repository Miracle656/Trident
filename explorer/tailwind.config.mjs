import animate from 'tailwindcss-animate';

/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,ts,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        mono: ['ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
      },
      colors: {
        ember: {
          400: '#ff6a42',
          500: '#ff4a1f',
          600: '#d63a12',
          700: '#a82c0c',
        },
        signal: {
          400: '#4ce08a',
          500: '#2fd575',
          600: '#1fae5c',
        },
      },
      keyframes: {
        'gradient-pan': {
          '0%': { 'background-position': '0% 50%' },
          '50%': { 'background-position': '100% 50%' },
          '100%': { 'background-position': '0% 50%' },
        },
      },
      animation: {
        'gradient-pan': 'gradient-pan 8s ease infinite',
      },
    },
  },
  plugins: [animate],
};
