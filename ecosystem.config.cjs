module.exports = {
  apps: [
    {
      name: 'acws',
      script: 'index.js',
      interpreter: 'node',
      interpreter_args: '--env-file=.env',
      cwd: '/opt/acws',
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      max_restarts: 10,
      restart_delay: 5000,
      watch: false,
      env: {
        NODE_ENV: 'production',
      },
    },
  ],
};
