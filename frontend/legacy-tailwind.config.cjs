module.exports = {
  content: ['../templates/**/*.html', '../static/js/index.js'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        primary: '#58cc02',
        'background-light': '#ffffff', 'background-dark': '#ffffff',
        'card-light': '#ffffff', 'card-dark': '#ffffff',
        'text-primary-light': '#3c3c3c', 'text-primary-dark': '#3c3c3c',
        'text-secondary-light': '#707070', 'text-secondary-dark': '#707070',
        'neutral-gray': '#707070', 'near-black': '#3c3c3c',
        destructive: '#ff4b4b', 'destructive-red': '#ff4b4b',
      },
      fontFamily: { display: ['var(--ui-font)', 'sans-serif'] },
    },
  },
  plugins: [require('@tailwindcss/forms')],
};
