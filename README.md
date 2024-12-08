# Crontab Jobs

A Node.js application that monitors directories and processes new files automatically using crontab scheduling.

## Features

- **File Processing**: Automatically detects and processes new files in configured directories
- **Database Integration**: Stores processing results in PostgreSQL database
- **Configurable**: Easy configuration through `config.ini` file without code changes
- **Logging**: Daily logging with [Pino](https://github.com/pinojs/pino) for both successful and failed operations
- **Process Management**: Uses [PM2](https://pm2.keymetrics.io/) for stable process management in Linux environments
- **Flexible Scheduling**: Customizable cron schedules through configuration

## Configuration

All settings are managed through `config.ini` file:

### Required Configuration Parameters

1. **Directory Settings**
   - Watch directory paths

2. **Database Connection**
   - Host address
   - Database name
   - Table name
   - Username
   - Password

3. **Cron Schedule**
   - Customizable schedule timing based on your requirements

## Installation

```bash
npm install
```

## Usage

1. Configure your settings in `config.ini`
2. Start the application:
   ```bash
   pm2 start app.js
   ```

## Logging

Logs are generated daily and include:
- Success records
- Failed operations
- Processing status

## Contributing

Feel free to submit issues and pull requests.

## License

[FYI](LICENSE)
