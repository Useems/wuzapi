module.exports = {
  apps: [
    {
      name: "wuzapi",
      script: "./wuzapi",
      args: "-logtype=console -color=true",
      interpreter: "none",
      watch: false,
      autorestart: true,
      restart_delay: 5000,
      env_file: ".env"
    }
  ]
};