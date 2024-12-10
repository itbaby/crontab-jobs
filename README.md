# Crontab Jobs

A Node.js application that processes CSV files containing spam, fraud, and TCPA data, tracking their occurrences and maintaining historical records in PostgreSQL.

## Features

- **CSV Processing**: Processes CSV files containing spam scores, fraud probability, and TCPA data
- **Database Integration**: Uses [postgres](https://github.com/porsager/postgres) for efficient PostgreSQL operations
- **State Management**: Uses [lowdb](https://github.com/typicode/lowdb) for tracking processed records
- **Date Processing**: Integrates [date-fns](https://date-fns.org/) for reliable date operations
- **Data Tracking**: Maintains first and last occurrence dates for spam, fraud, and TCPA events
- **Incremental Processing**: Supports resuming file processing from last processed record

## Data Processing

The application processes three main types of data:
1. **Spam Records**: Based on SpamScore categories (MAYBE, PROBABLY, ALMOST_CERTAINLY, DEFINITELY)
2. **Fraud Records**: Based on FraudProbability
3. **TCPA Records**: Based on TCPAFraudProbability

For each phone number, it tracks:
- First and last occurrence dates for each type
- Count of occurrences for each type
- Creation date of the first spam record

## Configuration

Settings are managed through `config/config_basic.ini`:

### Required Configuration

1. **Database Connection**
   ```ini
   [database]
   host=your_host
   port=your_port
   user=your_user
   password=your_password
   database=your_database
   ```

2. **Path Configuration**
   ```ini
   [path]
   watchdir=path_to_csv_files
   ```

## Database Schema

The application uses the following PostgreSQL table structure:

```sql
CREATE TABLE public.spams (
    number text PRIMARY KEY,
    spam_count integer,
    fraud_count integer,
    tcpa_count integer,
    first_spam_on date,
    last_spam_on date,
    first_fraud_on date,
    last_fraud_on date,
    first_tcpa_on date,
    last_tcpa_on date,
    created_on date
);
```

## Installation

```bash
npm install
```

## Dependencies

- postgres
- lowdb
- date-fns
- fast-csv
- walk
- lodash-es

## Usage

1. Configure your database and path settings in `config/config_basic.ini`
2. Run the application:
   ```bash
   node watcher.js
   ```

## State Management

The application uses `logs.json` to track:
- Processed files
- Number of records processed per file
- Prevents duplicate processing of records

## Error Handling

- Maintains transaction integrity for database operations
- Logs processing errors with file and record details
- Supports resuming from last successful record in case of failures
